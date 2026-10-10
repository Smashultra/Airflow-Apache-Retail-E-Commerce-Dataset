"""RFM dùng chung: cập nhật dữ liệu tích lũy và chạy phân nhóm daily.

Training import helper từ file này; chỉ main() bên dưới chạy daily.
"""
import argparse
import logging
import hashlib
import json
import re
from uuid import uuid4
from datetime import date, timedelta
from functools import reduce
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import StandardScaler, StandardScalerModel, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("ecommerce_rfm")
DEFAULT_KMEANS_SEED = 42
DEFAULT_NUM_BUCKETS = 64
# Đường dẫn mặc định theo project, không phụ thuộc thư mục gọi lệnh.
DATA_ROOT = Path(__file__).resolve().parents[1] / "data"
LOG_FEATURES = ["Recency_log", "Frequency_log", "Monetary_log"]
# k is selected explicitly during training, never by the daily job.

# 1. Tham số chạy daily và kiểm tra ngày chốt.
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Compute RFM metrics from curated transactions."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Cleaned seed or new batch; requires --batch-id.",
    )
    parser.add_argument(
        "--output",
        default=str(DATA_ROOT / "rfm"),
        help="Path to the RFM output directory.", # nơi lưu kết quả RFM
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Analysis cutoff date in YYYY-MM-DD format.",
    )
    parser.add_argument("--model-path", required=True, # model path là thư mục chứa PipelineModel đã training
                        help="Path to the fitted RFM PipelineModel.")
    parser.add_argument("--model-version", # model version là nhãn version được ghi vào output
                        help="Optional label; defaults to the model directory name.")
    add_store_arguments(parser, daily=True)
    args = parser.parse_args(argv)
    validate_store_arguments(parser, args, daily=True)
    if not args.model_path.strip() or not args.output.strip():
        parser.error("--model-path and --output must not be blank.")
    # Ví dụ models/v1 hoặc models/v1/ đều cho model_version = v1.
    if args.model_version is None:
        args.model_version = args.model_path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    if not args.model_version.strip() or args.model_version in (".", ".."):
        parser.error("Provide a model directory name or an explicit --model-version.")
    return args


def add_store_arguments(parser, daily=False):
    """Các tham số cho curated incremental và kỳ phân nhóm."""
    parser.add_argument('--store-path', default=str(DATA_ROOT / 'curated' / 'rfm_store'), help='Incremental curated transactions and customer state.')
    parser.add_argument('--batch-id', required=daily, help='Stable ID; reuse the same ID when retrying a batch.')
    parser.add_argument('--initialize-store', action='store_true')
    # None: khởi tạo dùng mặc định; store cũ tự lấy số bucket đã lưu.
    parser.set_defaults(num_buckets=None)
    if daily:
        parser.add_argument('--period-start', help='Inclusive UTC activity date; default run-date minus one day.')
        parser.add_argument('--all-customers', action='store_true',
                            help='Explicitly score every historical customer instead of active customers.')


def validate_store_arguments(parser, args, daily=False):
    """Kiểm tra chế độ chạy và khoảng ngày xử lý."""
    try:
        if date.fromisoformat(args.run_date).isoformat() != args.run_date:
            raise ValueError()
    except ValueError:
        parser.error('--run-date must be a valid date in YYYY-MM-DD format.')
    if args.input is not None and not args.input.strip():
        parser.error('--input must not be blank.')
    if args.store_path is not None and not args.store_path.strip():
        parser.error('--store-path must not be blank.')
    if bool(args.input) != bool(args.batch_id):
        parser.error('Store ingestion requires both --input and --batch-id.')
    if args.batch_id and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', args.batch_id):
        parser.error('Invalid --batch-id.')
    if args.initialize_store and not args.input:
        parser.error('--initialize-store requires --input and --batch-id.')
    if daily:
        try:
            start = date.fromisoformat(args.period_start) if args.period_start else date.fromisoformat(args.run_date) - timedelta(days=1)
            if args.period_start and start.isoformat() != args.period_start:
                raise ValueError()
            if start >= date.fromisoformat(args.run_date):
                raise ValueError()
        except ValueError:
            parser.error('--period-start must be an ISO date earlier than --run-date.')
        args.period_start = start.isoformat()


# 2. Kiểm tra dữ liệu đầu vào.
def validate_curated_contract(df: DataFrame) -> None:
    # Required columns for RFM aggregation.
    required_columns = {
        "CustomerID",
        "InvoiceNo",
        "InvoiceDate",
        "Quantity",
        "UnitPrice",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Data is missing required columns: {sorted(missing_columns)}"
        )

    # The cleaning job must normalize curated data to one stable schema.
    expected_types = {
        "CustomerID": StringType(),
        "InvoiceNo": StringType(),
        "InvoiceDate": TimestampType(),
        "Quantity": IntegerType(),
        "UnitPrice": DoubleType(),
    }

    for column_name, expected_type in expected_types.items():
        actual_type = df.schema[column_name].dataType

        if actual_type != expected_type:
            raise ValueError(
                f"Column '{column_name}' has type "
                f"{actual_type.simpleString()}; "
                f"expected: {expected_type.simpleString()}."
            )

    if df.isEmpty():
        raise ValueError("Input data is empty.")

    # Required columns must not contain null values.
    invalid_row = (
        F.col("CustomerID").isNull()
        | F.col("InvoiceNo").isNull()
        | F.col("InvoiceDate").isNull()
        | F.col("Quantity").isNull()
        | F.col("UnitPrice").isNull()
    )

    # Customer and invoice identifiers must not be blank.
    invalid_row = (
        invalid_row
        | (F.trim(F.col("CustomerID").cast("string")) == "")
        | (F.trim(F.col("InvoiceNo")) == "")
    )

    # This assertion ensures that the cleaning job handled invalid rows.
    invalid_row = (
        invalid_row
        | (F.col("Quantity") <= 0)
        | (F.col("UnitPrice") <= 0)
        | F.isnan("UnitPrice")
        | (F.abs(F.col("UnitPrice")) == F.lit(float("inf")))
    )

    if not df.filter(invalid_row).isEmpty():
        raise ValueError(
            "Curated data contains invalid values: required columns are "
            "null, customer/invoice identifiers are blank, or "
            "Quantity/UnitPrice are not positive finite values. The cleaning job "
            "did not satisfy the curated data contract."
        )


def normalize_transaction_rows(df: DataFrame) -> DataFrame:
    """Exact-row dedup, retaining all source business columns and invoice lines."""
    validate_curated_contract(df)
    if "batch_id" in df.columns:
        raise ValueError("batch_id is reserved for batch metadata.")
    # Partition dates are derived metadata, not a transaction identity.
    # Recompute rather than trust stale upstream partition labels.
    return (df.withColumn("year", F.year("InvoiceDate"))
            .withColumn("month", F.month("InvoiceDate"))
            .dropDuplicates())


def _assert_transaction_schema(left: DataFrame, right: DataFrame) -> None:
    def types(df):
        return {field.name: field.dataType for field in df.schema.fields}
    if types(left) != types(right):
        raise ValueError("Stored/input transaction schemas must match, including business columns.")


def _store_filesystem(spark: SparkSession, store_path: str):
    path = spark._jvm.org.apache.hadoop.fs.Path(store_path)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.getUri().getScheme() not in ("file", "hdfs"):
        raise ValueError("The incremental store requires file/HDFS atomic rename semantics.")
    return fs, fs.makeQualified(path)


def assert_separate_paths(spark: SparkSession, first: str, second: str) -> None:
    """Prevent input/store from overlapping paths that a job can replace."""
    def qualify(value):
        path = spark._jvm.org.apache.hadoop.fs.Path(value)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        uri = fs.makeQualified(path).toUri().normalize()
        return str(uri.getScheme()), str(uri.getAuthority()), str(uri.getPath()).rstrip("/")
    a, b = qualify(first), qualify(second)
    if a[:2] == b[:2] and (a[2] == b[2] or a[2].startswith(b[2] + "/")
                          or b[2].startswith(a[2] + "/")):
        raise ValueError("Input/store/output/model paths must be separate and non-overlapping.")


# 3. Lọc giao dịch trước ngày chốt trong chế độ snapshot.
def select_history(
    df: DataFrame,
    run_date: str,
) -> DataFrame:
    cutoff = F.lit(run_date).cast("date")
    cutoff_date = date.fromisoformat(run_date)

    if {"year", "month"}.issubset(df.columns):
        df = df.filter(
            (F.col("year") < F.lit(cutoff_date.year))
            | ((F.col("year") == F.lit(cutoff_date.year))
            & (F.col("month") <= F.lit(cutoff_date.month)))
        )

    history = df.filter(
        F.col("InvoiceDate") < cutoff
    )

    if history.isEmpty():
        raise ValueError(
            f"No transactions exist before the cutoff date {run_date}."
        )

    return history

# 4. Curated incremental: giao dịch, hóa đơn và state theo khách.

def canonical_transactions(df):
    """Kiểm tra dữ liệu sạch; bỏ cột partition khỏi khóa chống trùng."""
    normalized = normalize_transaction_rows(df)
    if 'transaction_date' in df.columns or '_customer_bucket' in df.columns:
        raise ValueError('transaction_date/_customer_bucket are reserved store columns.')
    return normalized.drop('year', 'month').dropDuplicates()


def customer_state(transactions):
    """Khởi tạo F, tổng tiền và ngày mua cuối từ lịch sử."""
    amount = F.col('Quantity').cast('decimal(18,4)') * F.col('UnitPrice').cast('decimal(18,4)')
    return transactions.groupBy('CustomerID').agg(
        F.max('InvoiceDate').alias('LastPurchaseDate'),
        F.countDistinct('InvoiceNo').alias('Frequency'),
        F.sum(amount).alias('MonetaryTotal'),
    )


def state_to_rfm(state, run_date):
    """Lấy F/M từ state; chỉ tính Recency và làm tròn tiền, không cộng lại."""
    return state.select(
        'CustomerID', F.lit(run_date).cast('date').alias('run_date'),
        'LastPurchaseDate',
        F.datediff(F.lit(run_date).cast('date'), F.to_date('LastPurchaseDate')).alias('Recency'),
        'Frequency', F.round('MonetaryTotal', 2).alias('Monetary'),
    )


def active_customers(transactions, period_start, run_date):
    """Chọn khách mua trong [period_start, run_date)."""
    return transactions.where(
        (F.col('InvoiceDate') >= F.lit(period_start).cast('timestamp')) &
        (F.col('InvoiceDate') < F.lit(run_date).cast('timestamp'))
    ).select('CustomerID').distinct()


def advance_state(old_state, old_invoices, novel):
    """Cập nhật state bằng các dòng thực sự mới; không đếm lại hóa đơn cũ."""
    # novel chỉ chứa giao dịch chưa từng được ghi nhận.
    invoices = novel.select('CustomerID', 'InvoiceNo').distinct()
    if old_state is None:
        return customer_state(novel), invoices
    # Một hóa đơn có thể có dòng sản phẩm đến ở nhiều batch.
    new_invoices = invoices.join(old_invoices, ['CustomerID', 'InvoiceNo'], 'left_anti')
    counts = new_invoices.groupBy('CustomerID').agg(F.count('*').alias('_new_frequency'))
    amount = F.col('Quantity').cast('decimal(18,4)') * F.col('UnitPrice').cast('decimal(18,4)')
    increments = novel.groupBy('CustomerID').agg(
        F.max('InvoiceDate').alias('_last'), F.sum(amount).alias('_money'))
    increments = increments.join(counts, 'CustomerID', 'left')
    # Khách cũ được cộng phần mới; khách mới được thêm vào state.
    updated = old_state.join(increments, 'CustomerID', 'full').select(
        'CustomerID', F.greatest('LastPurchaseDate', '_last').alias('LastPurchaseDate'),
        (F.coalesce(F.col('Frequency'), F.lit(0)) + F.coalesce(F.col('_new_frequency'), F.lit(0))).cast('long').alias('Frequency'),
        (F.coalesce(F.col('MonetaryTotal'), F.lit(0)).cast('decimal(37,8)') +
         F.coalesce(F.col('_money'), F.lit(0)).cast('decimal(37,8)')).cast('decimal(38,8)').alias('MonetaryTotal'),
    )
    return updated, old_invoices.unionByName(invoices).dropDuplicates()


def _fingerprint(df):
    """Checksum của batch để phát hiện retry với nội dung khác."""
    rows = df.select(*sorted(df.columns)).toJSON()
    modulus = 1 << 256
    def partition(values):
        count, total = 0, 0
        for value in values:
            count += 1
            total = (total + int(hashlib.sha256(value.encode('utf-8')).hexdigest(), 16)) % modulus
        yield count, total
    count, total = rows.mapPartitions(partition).fold((0, 0), lambda a, b: (a[0]+b[0], (a[1]+b[1]) % modulus))
    schema = {f.name: f.dataType.jsonValue() for f in df.schema.fields}
    return {'count': count, 'sha256_sum': hex(total), 'schema': schema}


def _read_json(spark, fs, path):
    """Đọc manifest: danh sách đường dẫn của phiên bản đã hoàn tất."""
    stream = fs.open(path)
    try:
        text = spark._jvm.org.apache.commons.io.IOUtils.toString(stream, 'UTF-8')
        return json.loads(str(text))
    finally:
        stream.close()


def _write_json(fs, path, obj):
    """Ghi manifest trước khi công bố phiên bản mới."""
    stream = fs.create(path, False)
    try:
        stream.write(bytearray(json.dumps(obj, sort_keys=True).encode('utf-8')))
    finally:
        stream.close()


def _manifests(spark, fs, root):
    """Liệt kê các manifest đã commit, bỏ qua file tạm."""
    Path = spark._jvm.org.apache.hadoop.fs.Path
    path = Path(root, '_commits')
    if not fs.exists(path):
        return []
    return sorted([status.getPath() for status in fs.listStatus(path)
                   if status.isFile() and re.fullmatch(r'\d{12}\.json', status.getPath().getName())], key=str)


def load_manifest(spark, store_path, run_date=None, version=None):
    """Chọn snapshot không vượt ngày chốt, hoặc đúng version khi retry."""
    fs, root = _store_filesystem(spark, store_path)
    paths = _manifests(spark, fs, root)
    for path in reversed(paths):
        if version and path.getName() != version + '.json':
            continue
        manifest = _read_json(spark, fs, path)
        if run_date is None or manifest['run_date'] <= run_date:
            return manifest
        if version:
            raise ValueError('Store version includes data beyond requested run-date.')
    raise ValueError('No committed store snapshot for this cutoff; initialize or rebuild historical snapshot.')


def read_store_table(spark, manifest, table):
    """Đọc bảng state/invoices/transactions theo manifest."""
    if table == 'transactions':
        paths = [path for bucket in manifest['buckets'].values()
                 for path in bucket['transactions'].values()]
        # Read leaf partitions individually to avoid unrelated generation roots
        # being mistaken for a single Spark partitioned dataset.
        frames = [spark.read.parquet(path) for path in paths]
        return reduce(lambda a, b: a.unionByName(b), frames)
    paths = [bucket[table] for bucket in manifest['buckets'].values()]
    if not paths:
        raise ValueError('Empty store.')
    # State is compact (one row/customer). Transactions should be read by date.
    return spark.read.parquet(*paths)


def read_store_rfm(spark, store_path, run_date):
    """Đọc state rồi dựng RFM tại ngày chốt."""
    manifest = load_manifest(spark, store_path, run_date)
    return state_to_rfm(read_store_table(spark, manifest, 'state'), run_date), manifest


def read_store_active(spark, manifest, period_start, run_date):
    """Chỉ đọc giao dịch trong kỳ để tìm khách cần phân nhóm."""
    paths = [path for bucket in manifest['buckets'].values()
             for day, path in bucket['transactions'].items()
             if period_start <= day < run_date]
    if not paths:
        return read_store_table(spark, manifest, 'state').select('CustomerID').limit(0)
    transactions = reduce(lambda a, b: a.unionByName(b), [spark.read.parquet(path) for path in paths])
    return active_customers(transactions, period_start, run_date)


def _update_store_bucket(spark, fs, generation, key, delta, old):
    """Loại dòng đã có, cập nhật RFM tích lũy rồi ghi một bucket mới."""
    Path = spark._jvm.org.apache.hadoop.fs.Path
    dates = delta.select(F.to_date('InvoiceDate')).distinct().collect()
    days = sorted(str(row[0]) for row in dates)
    tx_map = dict(old['transactions']) if old else {}
    existing_paths = [tx_map[day] for day in days if day in tx_map]
    if existing_paths:
        frames = [spark.read.parquet(path) for path in existing_paths]
        existing = reduce(lambda a, b: a.unionByName(b), frames)
        _assert_transaction_schema(existing, delta)
        # Hai giá trị null ở cột phụ cũng được xem là giống nhau.
        condition = F.lit(True)
        for column_name in delta.columns:
            quoted_name = '`' + column_name.replace('`', '``') + '`'
            same_value = F.col('n.' + quoted_name).eqNullSafe(
                F.col('o.' + quoted_name)
            )
            condition = condition & same_value
        novel = delta.alias('n').join(existing.alias('o'), condition, 'left_anti')
        merged = existing.unionByName(delta).dropDuplicates()
    else:
        novel, merged = delta, delta
    novel = novel.cache()
    try:
        # Cộng các dòng mới vào state và danh sách hóa đơn.
        old_state = spark.read.parquet(old['state']) if old else None
        old_invoices = spark.read.parquet(old['invoices']) if old else None
        changed_state, invoices = advance_state(old_state, old_invoices, novel)
        base = Path(generation, 'customer_bucket=' + key)
        tx_path = str(Path(base, 'transactions'))
        state_path = str(Path(base, 'state'))
        invoice_path = str(Path(base, 'invoices'))
        # Ghi vào đường dẫn mới; phiên bản cũ vẫn đọc được.
        (merged.withColumn('transaction_date', F.to_date('InvoiceDate'))
         .write.mode('errorifexists')
         .partitionBy('transaction_date')
         .parquet(tx_path))
        changed_state.write.mode('errorifexists').parquet(state_path)
        invoices.write.mode('errorifexists').parquet(invoice_path)
        written_paths = (tx_path, state_path, invoice_path)
        if any(not fs.exists(Path(path + '/_SUCCESS')) for path in written_paths):
            raise ValueError('Bucket write did not complete.')
        for day in days:
            tx_map[day] = tx_path + '/transaction_date=' + day
        return {'transactions': tx_map, 'state': state_path, 'invoices': invoice_path}
    finally:
        novel.unpersist()


def _publish_store_manifest(spark, fs, root, manifest):
    """Công bố phiên bản chỉ sau khi mọi bucket đã ghi thành công."""
    Path = spark._jvm.org.apache.hadoop.fs.Path
    commits = Path(root, '_commits')
    fs.mkdirs(commits)
    pending = Path(root, '_pending_' + uuid4().hex + '.json')
    _write_json(fs, pending, manifest)
    destination = Path(commits, manifest['version'] + '.json')
    if fs.exists(destination) or not fs.rename(pending, destination):
        raise ValueError('Atomic manifest publication failed.')


def ingest_store(spark, input_path, store_path, batch_id, run_date,
                 initialize=False, num_buckets=64):
    """Đọc batch → kiểm tra retry → cập nhật bucket → công bố phiên bản."""
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', batch_id or ''):
        raise ValueError('Invalid batch-id.')
    if num_buckets is not None and not 1 <= num_buckets <= 4096:
        raise ValueError('Invalid bucket count.')
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError('Invalid run-date.')
    assert_separate_paths(spark, input_path, store_path)
    fs, root = _store_filesystem(spark, store_path)
    Path = spark._jvm.org.apache.hadoop.fs.Path
    fs.mkdirs(root)
    lock = Path(root, '_writer.lock')
    if not fs.createNewFile(lock):
        raise ValueError('Store locked; inspect previous writer before retrying.')
    # Chỉ một writer được cập nhật store tại một thời điểm.
    incoming = None
    try:
        # 1. Đọc và kiểm tra batch; không nhận giao dịch sau ngày chốt.
        incoming = canonical_transactions(spark.read.parquet(input_path)).cache()
        if incoming.where(F.col('InvoiceDate') >= F.lit(run_date).cast('timestamp')).limit(1).count():
            raise ValueError('Input contains transactions on/after run-date; split input or choose a later cutoff.')
        fingerprint = _fingerprint(incoming)
        paths = _manifests(spark, fs, root)
        previous = _read_json(spark, fs, paths[-1]) if paths else None
        if num_buckets is None:
            num_buckets = previous['num_buckets'] if previous else DEFAULT_NUM_BUCKETS
        # 2. Retry dùng snapshot cũ; batch mới phải đúng schema và cutoff.
        if previous:
            if batch_id in previous['batches']:
                entry = previous['batches'][batch_id]
                if entry['fingerprint'] != fingerprint or entry['run_date'] != run_date:
                    raise ValueError('Conflicting batch-id: content/cutoff changed.')
                return load_manifest(spark, store_path, version=entry['version'])
            if initialize:
                raise ValueError('Store already initialized.')
            if run_date < previous['run_date']:
                raise ValueError('Cannot ingest backwards; use a new store for historical rebuild.')
            if previous['num_buckets'] != num_buckets:
                raise ValueError('Bucket count cannot change after initialization.')
            if previous['schema'] != fingerprint['schema']:
                raise ValueError('Transaction business schema changed.')
        elif not initialize:
            raise ValueError('First ingestion requires --initialize-store and complete curated history.')
        version = f'{len(paths)+1:012d}'
        generation = Path(root, '_generations/' + version + '_' + uuid4().hex)
        bucket_map = dict(previous['buckets']) if previous else {}
        tagged = incoming.withColumn('_customer_bucket', F.pmod(F.xxhash64('CustomerID'), F.lit(num_buckets))).cache()
        try:
            touched_buckets = sorted(row[0] for row in tagged.select('_customer_bucket').distinct().collect())
            # 3. Chỉ cập nhật bucket và ngày có trong batch mới.
            for bucket in touched_buckets:
                key = str(bucket)
                delta = tagged.where(F.col('_customer_bucket') == bucket).drop('_customer_bucket')
                old = bucket_map.get(key)
                bucket_map[key] = _update_store_bucket(
                    spark, fs, generation, key, delta, old)
        finally:
            tagged.unpersist()
        # 4. Chỉ công bố manifest sau khi mọi bảng đã ghi xong.
        batches = dict(previous['batches']) if previous else {}
        batches[batch_id] = {'fingerprint': fingerprint, 'run_date': run_date, 'version': version}
        manifest = {'format_version': 1, 'version': version, 'run_date': run_date,
                    'num_buckets': num_buckets, 'schema': fingerprint['schema'],
                    'buckets': bucket_map, 'batches': batches}
        _publish_store_manifest(spark, fs, root, manifest)
        return manifest
    finally:
        # Giải phóng cache và lock của chính lần chạy này.
        if incoming is not None:
            incoming.unpersist()
        fs.delete(lock, False)


# 5. Tính RFM từ toàn bộ giao dịch (chế độ snapshot cũ).
def compute_rfm(
    history: DataFrame,
    run_date: str,
) -> DataFrame:
    """Aggregate prefiltered history; cast prices to 4 decimal places,
    then round each customer total to 2 places. Zero after rounding is allowed.
    """

    transactions = history.withColumn(
        "LineTotal",
        (
            F.col("Quantity").cast("decimal(18, 4)")
            * F.col("UnitPrice").cast("decimal(18, 4)")
        ),
    )

    rfm = (
        transactions
        .groupBy("CustomerID")
        .agg(
            F.max("InvoiceDate").alias("LastPurchaseDate"),
            F.countDistinct("InvoiceNo").alias("Frequency"),
            F.round(
                F.sum("LineTotal"), 2
            ).alias("Monetary"),
        )
        .withColumn(
            "Recency",
            F.datediff(
                F.lit(run_date).cast("date"),
                F.to_date("LastPurchaseDate"),
            ),
        )
        .withColumn(
            "run_date",
            F.lit(run_date).cast("date"),
        )
    )

    return rfm.select(
        "CustomerID",
        "run_date",
        "LastPurchaseDate",
        "Recency",
        "Frequency",
        "Monetary",
    )


# 6. Điểm RFM, log features và scoring bằng model đã học.
def add_rfm_scores(rfm: DataFrame) -> DataFrame:
    """Preserve ties using percent_rank; CustomerID never affects scores.

    Rank intervals [0, .2), [.2, .4), [.4, .6), [.6, .8), [.8, 1]
    map to scores 1 through 5. A constant metric or a single customer
    receives score 1, with no neutral-score override. Groups may be uneven
    and some scores may be absent. Scores can change across analysis dates.
    Input must contain valid metrics and one row per customer.
    """
    result = rfm
    for metric, score_name, ascending in (
        ("Recency", "R_score", False),
        ("Frequency", "F_score", True),
        ("Monetary", "M_score", True),
    ):
        order = F.col(metric).asc() if ascending else F.col(metric).desc()
        rank = F.percent_rank().over(Window.orderBy(order))
        score = (
            F.when(rank < 0.2, 1)
            .when(rank < 0.4, 2)
            .when(rank < 0.6, 3)
            .when(rank < 0.8, 4)
            .otherwise(5)
        )
        result = result.withColumn(score_name, score)

    return result.withColumn(
        "RFM_score",
        F.concat(*(F.col(c).cast("string")
                   for c in ("R_score", "F_score", "M_score"))),
    )

# Shared log features for offline k selection, training and daily scoring.
def prepare_kmeans_features(rfm: DataFrame) -> DataFrame:
    """Validate metrics and apply stateless log1p; never fit a scaler here."""
    invalid = F.lit(False)
    for name in ("Recency", "Frequency", "Monetary"):
        value = F.col(name).cast("double")
        invalid = (invalid | value.isNull() | F.isnan(value)
                   | (F.abs(value) == F.lit(float("inf"))) | (value < 0))
    invalid = invalid | (F.col("Recency") < 1) | (F.col("Frequency") < 1)
    if rfm.isEmpty() or not rfm.filter(invalid).isEmpty():
        raise ValueError("Invalid RFM features: nonempty, finite R/F >= 1 and M >= 0 required.")
    return (
        rfm
        .withColumn("Recency_log", F.log1p(F.col("Recency").cast("double")))
        .withColumn("Frequency_log", F.log1p(F.col("Frequency").cast("double")))
        .withColumn("Monetary_log", F.log1p(F.col("Monetary").cast("double")))
    )


def kmeans_feature_pipeline() -> Pipeline:
    """Assembles the *_log columns into a vector and standard-scales them
    (no KMeans stage). Fit once and reuse across every k you try.
    """
    assembler = VectorAssembler(
        inputCols=LOG_FEATURES,
        outputCol="raw_features",
    )
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features",
        withMean=True, withStd=True,
    )
    return Pipeline(stages=[assembler, scaler])


# 5b. Load the fitted training pipeline and score current customers.
def load_rfm_model(model_path: str) -> PipelineModel:
    """Fail fast if the artifact does not follow the shared feature contract."""
    model = PipelineModel.load(model_path)
    stages = model.stages
    # check số lượng và loại stages
    if (len(stages) != 3
            or not isinstance(stages[0], VectorAssembler)
            or not isinstance(stages[1], StandardScalerModel)
            or not isinstance(stages[2], KMeansModel)):
        raise ValueError("RFM model must contain assembler, fitted scaler and fitted KMeans.")
    # check cấu hình features
    assembler, scaler, kmeans = stages
    if (assembler.getInputCols() != LOG_FEATURES
            or assembler.getOutputCol() != "raw_features"
            or scaler.getInputCol() != "raw_features"
            or scaler.getOutputCol() != "features"
            or not scaler.getWithMean() or not scaler.getWithStd()
            or kmeans.getFeaturesCol() != "features"
            or kmeans.getPredictionCol() != "Cluster"
            or kmeans.getK() < 2):
        raise ValueError("RFM model preprocessing/Cluster contract does not match this job.")
    return model # trả về model hợp lệ để daily transform


def score_rfm_with_model(rfm: DataFrame, model: PipelineModel,
                         model_version: str) -> DataFrame:
    """Reuse saved mean/std and centroids; sparse daily populations are allowed."""
    if not model_version.strip():
        raise ValueError("model_version must not be blank.")
    clustered = model.transform(prepare_kmeans_features(rfm))
    return (clustered.drop(*LOG_FEATURES, "raw_features", "features")
            .withColumn("model_version", F.lit(model_version)))


def add_rfm_segment(rfm: DataFrame, model_path: str,
                    model_version: str) -> DataFrame:
    """Daily entry point: load + transform only; never fit/retrain."""
    if not model_path.strip() or not model_version.strip():
        raise ValueError("model_path and model_version must not be blank.")
    return score_rfm_with_model(rfm, load_rfm_model(model_path), model_version)


# Check results before writing to output folder
def validate_rfm_output(result: DataFrame, run_date: str, k: int,
                        model_version: str | None = None) -> None:
    """Reject invalid output before replacing a daily partition."""
    if k < 2:
        raise ValueError("k must be at least 2 for K-means segmentation.")
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError("run_date must use YYYY-MM-DD.")
    required = {
        "CustomerID", "run_date", "LastPurchaseDate", "Recency",
        "Frequency", "Monetary", "R_score", "F_score", "M_score", "RFM_score",
        "Cluster", "model_version",
    }
    missing = required - set(result.columns)
    if missing:
        raise ValueError(f"RFM output is missing columns: {sorted(missing)}")
    if result.isEmpty():
        raise ValueError("RFM output is empty.")

    cluster_dtype = result.schema["Cluster"].dataType
    if not isinstance(cluster_dtype, (IntegerType, LongType)):
        raise ValueError(
            f"Cluster must have an integer type; got {cluster_dtype.simpleString()}."
        )

    if not isinstance(result.schema["model_version"].dataType, StringType):
        raise ValueError("model_version must have a string type.")
    invalid_version = (F.col("model_version").isNull()
                       | (F.trim(F.col("model_version")) == ""))
    if model_version is not None:
        if not model_version.strip():
            raise ValueError("model_version must not be blank.")
        invalid_version = invalid_version | (F.col("model_version") != model_version)
    if (not result.filter(invalid_version).isEmpty()
            or result.select("model_version").distinct().count() != 1):
        raise ValueError("RFM output contains invalid or inconsistent model_version.")

    cutoff = F.lit(run_date).cast("date")
    invalid = (
        F.col("CustomerID").isNull()
        | (F.trim(F.col("CustomerID")) == "")
        | F.col("run_date").isNull()
        | (F.col("run_date") != cutoff)
        | F.col("LastPurchaseDate").isNull()
        | (F.col("LastPurchaseDate") >= cutoff)
        | F.col("Recency").isNull()
        | (F.col("Recency") < 1)
        | (F.col("Recency") != F.datediff(cutoff, F.to_date("LastPurchaseDate")))
        | F.col("Frequency").isNull()
        | (F.col("Frequency") < 1)
        | F.col("Monetary").isNull()
        | (F.col("Monetary") < 0)
        | F.isnan(F.col("Monetary").cast("double"))
        | (F.abs(F.col("Monetary").cast("double")) == F.lit(float("inf")))
    )
    for column in ("R_score", "F_score", "M_score"):
        invalid = invalid | F.col(column).isNull() | (~F.col(column).isin(1, 2, 3, 4, 5))
    expected_code = F.concat(*(F.col(c).cast("string")
                              for c in ("R_score", "F_score", "M_score")))
    invalid = invalid | F.col("RFM_score").isNull() | (F.col("RFM_score") != expected_code)

    if not result.filter(invalid).isEmpty():
        raise ValueError("RFM output contains invalid metrics, scores, or run_date.")

    invalid_cluster = (
        F.col("Cluster").isNull() | (F.col("Cluster") < 0) | (F.col("Cluster") >= k)
    )
    if not result.filter(invalid_cluster).isEmpty():
        raise ValueError(
            f"RFM output contains invalid Cluster values (must be an integer in [0, {k}))."
        )

    if not result.groupBy("CustomerID").count().filter(F.col("count") > 1).isEmpty():
        raise ValueError("RFM output contains duplicate customers.")

# Ghi kết quả vào partition của ngày chốt.
def write_rfm(
    result: DataFrame, output_root: str, run_date: str, k: int,
    model_version: str | None = None,
) -> str:
    """Overwrite only the partition for the current cutoff date."""
    validate_rfm_output(result, run_date, k=k, model_version=model_version)
    output_path = f"{output_root.rstrip('/')}/run_date={run_date}"
    (
        result.drop("run_date")
        .write.mode("overwrite")
        .parquet(output_path)
    )
    return output_path


# 7. Daily: cập nhật state, chọn khách trong kỳ rồi phân nhóm.
def main():
    args = parse_args()
    spark = None
    result = None

    try:
        spark = (
            SparkSession.builder
            .appName("CalculateRFM")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )

        for first, second in (
            (args.store_path, args.output), (args.store_path, args.model_path),
            (args.output, args.model_path),
        ):
            assert_separate_paths(spark, first, second)
        assert_separate_paths(spark, args.input, args.output)
        assert_separate_paths(spark, args.input, args.model_path)
        manifest = ingest_store(
            spark, args.input, args.store_path, args.batch_id,
            args.run_date, args.initialize_store, args.num_buckets,
        )
        population = state_to_rfm(
            read_store_table(spark, manifest, 'state'), args.run_date)
        active = None if args.all_customers else read_store_active(
            spark, manifest, args.period_start, args.run_date)
        # Điểm RFM tham chiếu toàn bộ khách; KMeans dùng log RFM, không dùng điểm.
        # F/M đã tích lũy trong state, không cộng lại giao dịch ở đây.
        rfm = add_rfm_scores(population)
        if active is not None:
            rfm = rfm.join(active, 'CustomerID', 'left_semi')
        if rfm.isEmpty():
            logger.info('No active customers in [%s, %s); writing empty dated result.',
                        args.period_start, args.run_date)
            empty = (rfm.withColumn('Cluster', F.lit(None).cast('int'))
                     .withColumn('model_version', F.lit(args.model_version)))
            empty.drop('run_date').write.mode('overwrite').parquet(
                f"{args.output.rstrip('/')}/run_date={args.run_date}")
            return
        model = load_rfm_model(args.model_path)
        k = model.stages[-1].getK() # stage cuối KMeansModel, lấy số cụm đã được cấu hình trong model
        result = score_rfm_with_model( # chấm điểm và socring -> output gồm metrics, điểm, Cluster và version
            rfm, model, args.model_version
        ).cache()
        customer_count = result.count()

        if customer_count < 5:
            logger.warning(
                "Only %s customers found: relative scores use a small population "
                "and may not cover all five levels.",
                customer_count,
            )

        output_path = write_rfm(result, args.output, args.run_date, k=k,
                                model_version=args.model_version)
        logger.info("RFM job completed: customers=%s, output=%s",
                    customer_count, output_path)

    except Exception:
        logger.exception("RFM job failed.")
        raise

    finally:
        try:
            if result is not None:
                result.unpersist() # giải phóng dữ liệu cache
        finally:
            if spark is not None:
                spark.stop() # dừng SparkSession
            # finally bảo đảm vẫn cố dừng Spark ngay cả khi việc giải phóng cache gặp lỗi

if __name__ == "__main__":
    main()
