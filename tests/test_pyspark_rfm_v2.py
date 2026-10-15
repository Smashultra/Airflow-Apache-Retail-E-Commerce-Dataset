"""Test cho scripts/pyspark_rfm.py: RFM tích lũy (store), chấm điểm, phân cụm và job daily.

Bố cục (đi từ hàm nhỏ tới hàm lớn):
  0. Helper và fixture dùng chung
  1. CLI: parse_args / validate_store_arguments
  2. Hợp đồng dữ liệu đầu vào: validate_curated_contract
  3. Chuẩn hóa giao dịch và kiểm tra đường dẫn
  4. Chế độ snapshot: select_history, compute_rfm (dùng làm "đáp án" để đối chiếu)
  5. Điểm RFM: add_rfm_scores
  6. State tích lũy: customer_state, state_to_rfm, advance_state, daily_rfm_population, _fingerprint
  7. Đặc trưng và model: prepare_kmeans_features, load_rfm_model, add_rfm_segment
  8. Kiểm tra và ghi output: validate_rfm_output, write_rfm
  9. Store: manifest và ingest_store (chạy cả chế độ "memory" và "disk")
 10. main(): job daily end-to-end

Test KHÔNG import scripts/train_rfm_kmeans.py: model được fit trực tiếp bằng pipeline
dùng chung của pyspark_rfm. Các test của chính script training nằm ở test_train_rfm_kmeans.py.
"""
import argparse
import logging
import math
import runpy
import sys
import types
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pyspark")  # Thiếu PySpark thì pytest bỏ qua cả module (skip KHÔNG có nghĩa là đã kiểm tra thành công).

from pyspark.ml import Pipeline, PipelineModel  # noqa: E402
from pyspark.ml.clustering import KMeans, KMeansModel  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import scripts.pyspark_rfm as job  # noqa: E402
from scripts.pyspark_rfm import (  # noqa: E402
    DATA_ROOT,
    LOG_FEATURES,
    DEFAULT_KMEANS_SEED,
    DEFAULT_NUM_BUCKETS,
    _assert_transaction_schema,
    _fingerprint,
    add_rfm_scores,
    add_rfm_segment,
    apply_rfm_model,
    advance_state,
    assert_separate_paths,
    canonical_transactions,
    compute_rfm,
    customer_state,
    daily_rfm_population,
    fit_rfm_model,
    ingest_store,
    kmeans_feature_pipeline,
    load_manifest,
    load_rfm_model,
    normalize_transaction_rows,
    parse_args,
    prepare_kmeans_features,
    read_store_rfm,
    read_store_table,
    score_rfm_with_model,
    select_history,
    state_to_rfm,
    validate_curated_contract,
    validate_rfm_output,
    validate_store_arguments,
    write_rfm,
)

TX_COLUMNS = (
    "CustomerID string, InvoiceNo string, InvoiceDate timestamp, "
    "Quantity int, UnitPrice double"
)
STOCK_COLUMNS = TX_COLUMNS + ", StockCode string"
RFM_COLUMNS = "CustomerID string, Recency int, Frequency int, Monetary double"


# ---------------------------------------------------------------------------
# 0. HELPER VÀ FIXTURE DÙNG CHUNG
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():  # SparkSession local dùng chung cho module; cố ý KHÔNG stop() khi kết thúc để không làm hỏng session của các file test khác.
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-pyspark-rfm")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def utc(*parts):  # Tạo datetime có múi giờ UTC ngắn gọn, ví dụ utc(2011, 12, 9).
    return datetime(*parts, tzinfo=timezone.utc)


def curated_schema(customer_id_type=None):  # Schema chuẩn của dữ liệu curated (không null); đổi kiểu CustomerID để test vi phạm schema.
    return StructType([
        StructField("CustomerID", customer_id_type or StringType(), False),
        StructField("InvoiceNo", StringType(), False),
        StructField("InvoiceDate", TimestampType(), False),
        StructField("Quantity", IntegerType(), False),
        StructField("UnitPrice", DoubleType(), False),
    ])


def nullable_schema():  # Schema cho phép null: cần khi test giá trị null, vì curated_schema() không null sẽ bị Spark chặn ngay lúc tạo DataFrame, chưa tới validate_curated_contract.
    return StructType([
        StructField("CustomerID", StringType(), True),
        StructField("InvoiceNo", StringType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DoubleType(), True),
    ])


def valid_tx(spark):  # Một dòng giao dịch hợp lệ duy nhất, dùng làm "dữ liệu sạch" để làm hỏng từng điểm.
    return spark.createDataFrame(
        [("17850", "536365", utc(2026, 9, 1), 6, 2.55)], curated_schema()
    )


def history_sample(spark):  # Khách "a" có 3 dòng: hóa đơn i1 (2 dòng, 50 đ), i2 (60 đ) và 1 dòng "future" ngày 12/10 để test ranh giới ngày chốt.
    return spark.createDataFrame([
        ("a", "i1", utc(2011, 12, 1), 2, 10.),
        ("a", "i1", utc(2011, 12, 1), 1, 30.),
        ("a", "i2", utc(2011, 12, 9), 3, 20.),
        ("a", "future", utc(2011, 12, 10), 1, 999.),
    ], TX_COLUMNS)


def rfm_rows(spark, rows):  # Tạo DataFrame RFM thô từ list (CustomerID, Recency, Frequency, Monetary).
    return spark.createDataFrame(rows, RFM_COLUMNS)


def tx_frame(spark, rows):  # Tạo DataFrame giao dịch có StockCode; datetime không có tz được gắn UTC.
    rows = [
        tuple(v.replace(tzinfo=timezone.utc) if isinstance(v, datetime) else v for v in row)
        for row in rows
    ]
    return spark.createDataFrame(rows, STOCK_COLUMNS)


def seed(spark):  # Batch khởi tạo cho test store: khách A (giá 0.004 để thử làm tròn) và khách B.
    return tx_frame(spark, [
        ("A", "I1", datetime(2011, 12, 10, 9), 1, 0.004, "P1"),
        ("B", "I2", datetime(2011, 12, 9, 9), 2, 5.0, "P2"),
    ])


def extra_rows(spark):  # Batch bổ sung: thêm dòng sản phẩm mới vào hóa đơn I1 cũ, hóa đơn I3 của A và khách mới C.
    return tx_frame(spark, [
        ("A", "I1", datetime(2011, 12, 10, 9), 1, 0.004, "P3"),
        ("A", "I3", datetime(2011, 12, 11, 9), 1, 0.004, "P1"),
        ("C", "I4", datetime(2011, 12, 11, 9), 1, 20.0, "P1"),
    ])


def fit_model(spark, rows, k=2):  # Fit trực tiếp một PipelineModel (assembler + scaler + KMeans) đúng "hợp đồng" của job, không cần script training.
    prepared = prepare_kmeans_features(rfm_rows(spark, rows))
    stages = kmeans_feature_pipeline().getStages() + [
        KMeans(k=k, seed=42, featuresCol="features", predictionCol="Cluster")
    ]
    return Pipeline(stages=stages).fit(prepared)


BEST_AND_WORST = [  # 2 khách "xịn" (mới mua, mua nhiều, chi nhiều) và 2 khách "tệ" (lâu không mua, ít, rẻ): tách cụm rất rõ.
    ("best", 1, 20, 5000.0), ("best2", 2, 22, 4800.0),
    ("worst", 300, 1, 5.0), ("worst2", 310, 1, 8.0),
]


@pytest.fixture(scope="module")
def fitted_model(spark):  # Model KMeans k=2 đã fit trên 4 khách ở BEST_AND_WORST, dùng chung cho nhiều test.
    return fit_model(spark, BEST_AND_WORST)


@pytest.fixture
def valid_cluster_output(spark):  # Output daily hợp lệ gồm 1 khách; các test validator làm hỏng từng cột của nó (không cần train model).
    return spark.createDataFrame(
        [("a", date(2011, 12, 10), utc(2011, 12, 9), 1, 2, 110.0, 5, 5, 5, "555", 0)],
        "CustomerID string, run_date date, LastPurchaseDate timestamp, "
        "Recency int, Frequency int, Monetary double, R_score int, "
        "F_score int, M_score int, RFM_score string, Cluster int",
    ).withColumn("model_version", F.lit("v1"))


DAILY = ["--input", "new", "--batch-id", "b1", "--run-date", "2011-12-12",
         "--model-path", "models/v1"]  # Bộ tham số daily tối thiểu hợp lệ; các test CLI thêm/bớt từ đây.


# ---------------------------------------------------------------------------
# 1. CLI: parse_args / validate_store_arguments
# ---------------------------------------------------------------------------


def test_cli_accepts_valid_arguments():  # CLI nhận bộ tham số daily đầy đủ và trả về đúng giá trị đã truyền.
    args = parse_args(DAILY + ["--store-path", "store", "--output", "out",
                               "--model-version", "v1"])

    assert (args.run_date, args.model_path, args.model_version) == (
        "2011-12-12", "models/v1", "v1")
    assert (args.input, args.batch_id) == ("new", "b1")
    assert args.initialize_store is False


@pytest.mark.parametrize("day", ["2011-02-30", "20111212", "2011-1-1", "", "abc"])
def test_cli_rejects_invalid_run_date(day):  # --run-date sai (ngày không tồn tại, không có dấu gạch, thiếu số 0, rỗng, chữ) bị từ chối.
    argv = ["--input", "new", "--batch-id", "b1", "--run-date", day,
            "--model-path", "models/v1"]

    with pytest.raises(SystemExit):
        parse_args(argv)


@pytest.mark.parametrize("path,version", [
    ("models/v1", "v1"), ("models/v1/", "v1"),
    (r"C:\models\v1", "v1"), ("hdfs://host/models/v1", "v1"),
])
def test_cli_model_version_defaults_to_directory_name(path, version):  # Không truyền --model-version thì lấy tên thư mục cuối của --model-path (chịu được dấu "/" cuối, đường dẫn Windows và hdfs://).
    argv = ["--input", "new", "--batch-id", "b1", "--run-date", "2011-12-12",
            "--model-path", path]

    assert parse_args(argv).model_version == version


def test_cli_explicit_model_version_overrides_directory_name():  # --model-version tường minh được ưu tiên hơn tên thư mục.
    assert parse_args(DAILY + ["--model-version", "release1"]).model_version == "release1"


@pytest.mark.parametrize("model_path", ["models/..", "..", "."])
def test_cli_rejects_model_path_without_usable_directory_name(model_path):  # Đường dẫn model kết thúc bằng "." hoặc ".." không suy ra được version hợp lệ nên bị từ chối.
    argv = ["--input", "new", "--batch-id", "b1", "--run-date", "2011-12-12",
            "--model-path", model_path]

    with pytest.raises(SystemExit):
        parse_args(argv)


def test_cli_defaults_and_explicit_overrides():  # Mặc định store/output lấy từ DATA_ROOT, num_buckets là None (store cũ tự dùng số bucket đã lưu); giá trị truyền tường minh ghi đè mặc định.
    args = parse_args(DAILY)

    assert args.store_path == str(DATA_ROOT / "curated" / "rfm_store")
    assert args.output == str(DATA_ROOT / "rfm")
    assert args.num_buckets is None
    explicit = parse_args(DAILY + ["--model-version", "release1", "--output", "out",
                                   "--store-path", "existing"])
    assert (explicit.model_version, explicit.output, explicit.store_path) == (
        "release1", "out", "existing")


@pytest.mark.parametrize("extra", [
    ["--store-path", " "], ["--output", " "], ["--model-path", " "],
    ["--model-version", " "], ["--input", " "],
])
def test_cli_rejects_blank_values(extra):  # Các giá trị chỉ toàn khoảng trắng (store, output, model-path, model-version, input) bị từ chối; argparse lấy giá trị cuối nên truyền thêm sẽ ghi đè.
    with pytest.raises(SystemExit):
        parse_args(DAILY + extra)


@pytest.mark.parametrize("batch_id", ["", " ", "../bad", "a/b", "-start", "a" * 129])
def test_cli_rejects_invalid_batch_id(batch_id):  # batch-id phải bắt đầu bằng chữ/số, chỉ gồm chữ, số, "_" "-" và dài tối đa 128 ký tự (chặn path traversal).
    argv = ["--input", "new", "--batch-id", batch_id, "--run-date", "2011-12-12",
            "--model-path", "models/v1"]

    with pytest.raises(SystemExit):
        parse_args(argv)


def test_cli_accepts_batch_id_at_length_limit():  # batch-id đúng 128 ký tự vẫn hợp lệ (kiểm tra biên).
    argv = ["--input", "new", "--batch-id", "a" * 128, "--run-date", "2011-12-12",
            "--model-path", "models/v1"]

    assert parse_args(argv).batch_id == "a" * 128


@pytest.mark.parametrize("extra", [[], ["--input", "new"], ["--batch-id", "b1"]])
def test_daily_cli_requires_input_and_batch_id(extra):  # Chế độ daily bắt buộc có CẢ --input và --batch-id.
    with pytest.raises(SystemExit):
        parse_args(["--run-date", "2011-12-12", "--model-path", "models/v1"] + extra)


@pytest.mark.parametrize("extra", [
    [], ["--model-path", " "], ["--model-version", "v1"],
    ["--model-path", "m", "--model-version", "v1", "--k", "2"],
    ["--model-path", "m", "--model-version", " "],
])
def test_daily_cli_requires_model_and_rejects_legacy_k(extra):  # Daily bắt buộc có --model-path hợp lệ và không còn nhận --k (số cụm chỉ chọn lúc training).
    base = ["--input", "new", "--batch-id", "b1", "--store-path", "store",
            "--output", "out", "--run-date", "2011-12-10"]

    with pytest.raises(SystemExit):
        parse_args(base + extra)


def test_cli_initialize_store_flag_is_accepted():  # --initialize-store bật cờ khởi tạo store lần đầu (đi kèm --input/--batch-id có sẵn trong DAILY).
    assert parse_args(DAILY + ["--initialize-store"]).initialize_store is True


@pytest.mark.parametrize("extra", [
    ["--history-path", "old"], ["--initialize-history"], ["--num-buckets", "0"],
    ["--period-start", "2011-12-01"], ["--all-customers"],
])
def test_cli_rejects_removed_options(extra):  # Các cờ đã bỏ (history-path, initialize-history, --period-start, --all-customers: daily luôn chấm toàn bộ khách) và cờ không tồn tại (--num-buckets) đều bị argparse từ chối.
    with pytest.raises(SystemExit):
        parse_args(DAILY + extra)


def test_validate_store_arguments_initialize_requires_input():  # validate_store_arguments (dùng chung cho cả script training) từ chối --initialize-store khi không có --input/--batch-id.
    parser = argparse.ArgumentParser()
    args = argparse.Namespace(run_date="2011-12-12", input=None, store_path="store",
                              batch_id=None, initialize_store=True)

    with pytest.raises(SystemExit):
        validate_store_arguments(parser, args)


def test_validate_store_arguments_allows_read_only_mode():  # Không có input/batch-id và không khởi tạo (chế độ chỉ đọc store của training) thì hợp lệ.
    parser = argparse.ArgumentParser()
    args = argparse.Namespace(run_date="2011-12-12", input=None, store_path="store",
                              batch_id=None, initialize_store=False)

    validate_store_arguments(parser, args)


# ---------------------------------------------------------------------------
# 2. HỢP ĐỒNG DỮ LIỆU ĐẦU VÀO: validate_curated_contract
# ---------------------------------------------------------------------------


def test_validate_curated_contract_accepts_canonical_data(spark):  # Dữ liệu đúng schema chuẩn và giá trị sạch được chấp nhận.
    validate_curated_contract(valid_tx(spark))


def test_validate_curated_contract_accepts_extra_business_columns(spark):  # Cột thừa (như StockCode) không làm hợp đồng bị vi phạm.
    validate_curated_contract(valid_tx(spark).withColumn("StockCode", F.lit("P1")))


def test_validate_curated_contract_rejects_noncanonical_schema(spark):  # CustomerID kiểu số vi phạm schema chuẩn (phải là chuỗi) nên bị từ chối.
    df = spark.createDataFrame(
        [(17850, "536365", utc(2026, 9, 1), 6, 2.55)], curated_schema(IntegerType())
    )

    with pytest.raises(ValueError, match="CustomerID.*string"):
        validate_curated_contract(df)


@pytest.mark.parametrize("column,expr,expected", [
    ("Quantity", "CAST(Quantity AS BIGINT)", "int"),
    ("UnitPrice", "CAST(UnitPrice AS FLOAT)", "double"),
    ("InvoiceDate", "CAST(InvoiceDate AS STRING)", "timestamp"),
    ("InvoiceNo", "CAST(InvoiceNo AS INT)", "string"),
])
def test_validate_curated_contract_rejects_other_wrong_types(spark, column, expr, expected):  # Mọi cột bắt buộc đều phải đúng kiểu chuẩn; sai kiểu (bigint, float, string, int) báo lỗi nêu rõ kiểu mong đợi.
    df = valid_tx(spark).withColumn(column, F.expr(expr))

    with pytest.raises(ValueError, match=f"{column}.*{expected}"):
        validate_curated_contract(df)


def test_validate_curated_contract_rejects_missing_required_columns(spark):  # Thiếu cột bắt buộc (UnitPrice) thì báo lỗi nêu tên cột thiếu.
    with pytest.raises(ValueError, match="missing required columns.*UnitPrice"):
        validate_curated_contract(valid_tx(spark).drop("UnitPrice"))


def test_validate_curated_contract_rejects_empty_input(spark):  # Input rỗng (0 dòng) bị chặn thay vì chạy tiếp ra kết quả rỗng.
    with pytest.raises(ValueError, match="empty"):
        validate_curated_contract(spark.createDataFrame([], curated_schema()))


@pytest.mark.parametrize("row", [
    (None, "536365", utc(2026, 9, 1), 6, 2.55),
    ("17850", None, utc(2026, 9, 1), 6, 2.55),
    ("17850", "536365", None, 6, 2.55),
    ("17850", "536365", utc(2026, 9, 1), None, 2.55),
    ("17850", "536365", utc(2026, 9, 1), 6, None),
])
def test_validate_curated_contract_rejects_null_values(spark, row):  # Null ở từng cột bắt buộc (mỗi cột một ca riêng) đều bị từ chối.
    df = spark.createDataFrame([row], nullable_schema())

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)


@pytest.mark.parametrize("row", [
    ("   ", "536365", utc(2026, 9, 1), 6, 2.55),
    ("17850", "   ", utc(2026, 9, 1), 6, 2.55),
])
def test_validate_curated_contract_rejects_blank_ids(spark, row):  # CustomerID/InvoiceNo chỉ toàn khoảng trắng bị coi là rỗng, không phải chuỗi hợp lệ.
    df = spark.createDataFrame([row], curated_schema())

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)


@pytest.mark.parametrize("quantity,price", [(0, 2.55), (-1, 2.55), (6, 0.0), (6, -2.55)],
                         ids=["zero_quantity", "negative_quantity", "zero_price", "negative_price"])
def test_validate_curated_contract_rejects_nonpositive_values(spark, quantity, price):  # Quantity hoặc UnitPrice bằng 0 hay âm đều bị chặn (dữ liệu chưa được làm sạch).
    df = spark.createDataFrame(
        [("17850", "536365", utc(2026, 9, 1), quantity, price)], curated_schema()
    )

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)


@pytest.mark.parametrize("price", [float("nan"), float("inf"), -float("inf")])
def test_validate_curated_contract_rejects_nonfinite_price(spark, price):  # UnitPrice là NaN hoặc vô cực bị từ chối (không phải số dương hữu hạn).
    df = valid_tx(spark).withColumn("UnitPrice", F.lit(price))

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)


# ---------------------------------------------------------------------------
# 3. CHUẨN HÓA GIAO DỊCH VÀ KIỂM TRA ĐƯỜNG DẪN
# ---------------------------------------------------------------------------


def seed_rows(spark):  # Hai khách (a có hóa đơn "old", inactive có "i0") có StockCode, dùng cho test chuẩn hóa/hợp nhất.
    return spark.createDataFrame([
        ("a", "old", utc(2026, 9, 30), 1, 100., "p1"),
        ("inactive", "i0", utc(2026, 9, 20), 1, 50., "p0"),
    ], STOCK_COLUMNS)


def new_rows(spark):  # Batch mới của khách a: lặp lại hóa đơn "old" và thêm hóa đơn "new" gồm 2 dòng sản phẩm khác nhau (p2, p3).
    return spark.createDataFrame([
        ("a", "old", utc(2026, 9, 30), 1, 100., "p1"),
        ("a", "new", utc(2026, 10, 3), 2, 10., "p2"),
        ("a", "new", utc(2026, 10, 3), 2, 10., "p3"),
    ], STOCK_COLUMNS)


def test_merge_cumulative_rfm_overlap_and_distinct_invoice_lines(spark):  # Hợp nhất lịch sử + batch mới: dòng lặp hệt nhau chỉ giữ 1, nhưng dòng sản phẩm khác của cùng hóa đơn phải giữ; RFM tính đúng cho cả khách hoạt động và không hoạt động.
    incoming = new_rows(spark)
    repeated = incoming.union(incoming.filter(F.col("StockCode") == "p2"))

    merged = normalize_transaction_rows(seed_rows(spark).unionByName(repeated))

    assert merged.count() == 4
    rows = {r.CustomerID: r for r in compute_rfm(
        select_history(merged, "2026-10-04"), "2026-10-04").collect()}
    assert (rows["a"].Recency, rows["a"].Frequency, rows["a"].Monetary) == (
        1, 2, Decimal("140.00"))
    assert (rows["inactive"].Recency, rows["inactive"].Frequency,
            rows["inactive"].Monetary) == (14, 1, Decimal("50.00"))


def test_normalization_rebuilds_partition_dates(spark):  # normalize_transaction_rows tính lại year/month từ InvoiceDate thay vì tin nhãn partition cũ (sai) của upstream.
    incoming = seed_rows(spark).withColumn("year", F.lit(2099)).withColumn("month", F.lit(12))

    normalized = normalize_transaction_rows(incoming)

    assert {(r.year, r.month) for r in normalized.collect()} == {(2026, 9)}
    assert select_history(normalized, "2026-10-01").count() == 2


def test_normalization_dedups_rows_that_differ_only_by_stale_partition_labels(spark):  # Hai dòng giống hệt chỉ khác nhãn year/month cũ được coi là trùng sau khi tính lại partition.
    base = seed_rows(spark).limit(1)
    stale = base.withColumn("year", F.lit(1999)).withColumn("month", F.lit(1))
    other = base.withColumn("year", F.lit(2099)).withColumn("month", F.lit(12))

    assert normalize_transaction_rows(stale.unionByName(other)).count() == 1


def test_normalization_rejects_reserved_batch_id_column(spark):  # Cột batch_id được dành riêng cho metadata nên dữ liệu đầu vào không được chứa.
    with pytest.raises(ValueError, match="reserved"):
        normalize_transaction_rows(seed_rows(spark).withColumn("batch_id", F.lit("b")))


@pytest.mark.parametrize("column", ["transaction_date", "_customer_bucket"])
def test_canonical_transactions_rejects_reserved_store_columns(spark, column):  # canonical_transactions từ chối cột transaction_date/_customer_bucket vì store dùng chúng nội bộ.
    with pytest.raises(ValueError, match="reserved store columns"):
        canonical_transactions(seed_rows(spark).withColumn(column, F.lit("x")))


def test_canonical_transactions_drops_partition_columns(spark):  # canonical_transactions bỏ year/month khỏi kết quả để chúng không tham gia khóa chống trùng.
    result = canonical_transactions(seed_rows(spark))

    assert "year" not in result.columns and "month" not in result.columns
    assert result.count() == 2


def test_assert_transaction_schema_rejects_schema_drift(spark):  # Hai tập giao dịch khác schema nghiệp vụ (thiếu cột StockCode) không được hợp nhất.
    with pytest.raises(ValueError, match="schemas must match"):
        _assert_transaction_schema(seed_rows(spark), new_rows(spark).drop("StockCode"))


def test_assert_transaction_schema_accepts_same_schema_in_any_column_order(spark):  # Cùng tên cột và kiểu thì hợp lệ, không phụ thuộc thứ tự cột.
    left = seed_rows(spark)

    _assert_transaction_schema(left, left.select(*reversed(left.columns)))


@pytest.mark.parametrize("make_second", [
    lambda a: a,
    lambda a: a + "/",
    lambda a: a + "/child",
    lambda a: "file://" + a,
], ids=["identical", "trailing_slash", "child_of_first", "file_scheme_form"])
def test_assert_separate_paths_rejects_overlap(spark, tmp_path, make_second):  # Hai đường dẫn trùng hoặc lồng nhau (kể cả khác dấu "/" cuối hay dạng file://) bị từ chối để job không ghi đè dữ liệu của nhau.
    first = str(tmp_path / "store")

    with pytest.raises(ValueError, match="separate and non-overlapping"):
        assert_separate_paths(spark, first, make_second(first))


def test_assert_separate_paths_rejects_parent_of_other(spark, tmp_path):  # Đường dẫn thứ nhất là thư mục cha của đường dẫn thứ hai cũng bị từ chối (kiểm tra hai chiều).
    with pytest.raises(ValueError, match="separate and non-overlapping"):
        assert_separate_paths(spark, str(tmp_path), str(tmp_path / "child"))


def test_assert_separate_paths_allows_siblings_with_shared_prefix(spark, tmp_path):  # Hai thư mục anh em có tên bắt đầu giống nhau ("store" và "store2") không bị coi là lồng nhau.
    assert_separate_paths(spark, str(tmp_path / "store"), str(tmp_path / "store2"))


def test_store_filesystem_rejects_unsupported_scheme():  # Chỉ hỗ trợ file/HDFS (cần rename nguyên tử); scheme khác như s3a bị từ chối ngay.
    fake_fs = SimpleNamespace(getUri=lambda: SimpleNamespace(getScheme=lambda: "s3a"),
                              makeQualified=lambda p: p)
    fake_path = SimpleNamespace(getFileSystem=lambda conf: fake_fs)
    fake_spark = SimpleNamespace(
        _jvm=SimpleNamespace(org=SimpleNamespace(apache=SimpleNamespace(
            hadoop=SimpleNamespace(fs=SimpleNamespace(Path=lambda p: fake_path))))),
        _jsc=SimpleNamespace(hadoopConfiguration=lambda: None),
    )

    with pytest.raises(ValueError, match="atomic rename"):
        job._store_filesystem(fake_spark, "s3a://bucket/store")


# ---------------------------------------------------------------------------
# 4. CHẾ ĐỘ SNAPSHOT: select_history, compute_rfm
# ---------------------------------------------------------------------------


def test_select_history_rejects_no_transactions_before_cutoff(spark):  # Không có giao dịch nào trước ngày chốt thì báo lỗi, không trả bảng rỗng.
    df = spark.createDataFrame(
        [("17850", "536365", utc(2026, 9, 5), 6, 2.55)], TX_COLUMNS)

    with pytest.raises(ValueError, match="No transactions"):
        select_history(df, "2026-09-01")


def test_select_history_boundary_exact_cutoff(spark):  # Ranh giới ngày chốt: giao dịch 23:59:59 ngày trước được giữ, giao dịch đúng 00:00:00 ngày chốt bị loại.
    df = spark.createDataFrame([
        ("a", "i1", utc(2011, 12, 9, 23, 59, 59), 1, 10.),
        ("a", "i2", utc(2011, 12, 10, 0, 0, 0), 1, 20.),
    ], TX_COLUMNS)

    assert [r.InvoiceNo for r in select_history(df, "2011-12-10").collect()] == ["i1"]


def test_select_history_prunes_partitions_when_year_month_present(spark):  # Có cột year/month (dữ liệu partition) thì loại partition tương lai trước, nhưng kết quả cuối vẫn giống hệt chỉ lọc theo InvoiceDate.
    df = spark.createDataFrame([
        ("a", "i1", utc(2011, 8, 15), 1, 10., 2011, 8),
        ("a", "i2", utc(2011, 9, 5), 1, 20., 2011, 9),
        ("a", "i3", utc(2011, 9, 20), 1, 999., 2011, 9),   # cùng tháng chốt nhưng sau ngày chốt
        ("a", "i4", utc(2012, 1, 1), 1, 999., 2012, 1),    # partition ở tương lai
    ], TX_COLUMNS + ", year int, month int")

    assert {r.InvoiceNo for r in select_history(df, "2011-09-10").collect()} == {"i1", "i2"}


def test_select_history_without_partition_columns_still_filters_by_date(spark):  # Không có year/month thì chỉ lọc theo InvoiceDate (hành vi cơ bản).
    history = select_history(history_sample(spark), "2011-12-10")

    assert {r.InvoiceNo for r in history.collect()} == {"i1", "i2"}


def test_compute_rfm_separates_multiple_customers(spark):  # groupBy tách đúng theo từng khách, không lẫn Frequency/Monetary giữa các khách.
    df = spark.createDataFrame([
        ("a", "i1", utc(2011, 12, 1), 2, 10.),
        ("b", "i2", utc(2011, 12, 5), 1, 50.),
        ("b", "i3", utc(2011, 12, 8), 3, 20.),
    ], TX_COLUMNS)

    rfm = compute_rfm(select_history(df, "2011-12-10"), "2011-12-10").collect()

    assert {r.CustomerID: (r.Recency, r.Frequency, r.Monetary) for r in rfm} == {
        "a": (9, 1, Decimal("20.00")),
        "b": (2, 2, Decimal("110.00")),
    }


def test_compute_rfm_counts_distinct_invoices_and_sums_all_lines(spark):  # Frequency đếm số HÓA ĐƠN khác nhau (không phải số dòng) và Monetary cộng mọi dòng; output có đúng thứ tự cột.
    rfm = compute_rfm(select_history(history_sample(spark), "2011-12-10"), "2011-12-10")

    row = rfm.first()
    assert rfm.columns == ["CustomerID", "run_date", "LastPurchaseDate",
                           "Recency", "Frequency", "Monetary"]
    assert (row.Recency, row.Frequency, row.Monetary) == (1, 2, Decimal("110.00"))
    assert row.run_date == date(2011, 12, 10)


def test_compute_rfm_rounds_monetary_half_up(spark):  # Monetary làm tròn HALF_UP: 12.345 thành 12.35 (không phải 12.34).
    df = spark.createDataFrame([("a", "i1", utc(2011, 12, 1), 1, 12.345)], TX_COLUMNS)

    assert compute_rfm(select_history(df, "2011-12-10"), "2011-12-10").first().Monetary == Decimal("12.35")


def test_compute_rfm_rounds_after_summing_line_totals(spark):  # Cộng trước rồi làm tròn sau: 0.125 + 0.125 = 0.25 (không phải 0.13 + 0.13 = 0.26).
    df = spark.createDataFrame([
        ("a", "i1", utc(2011, 12, 1), 1, 0.125),
        ("a", "i2", utc(2011, 12, 2), 1, 0.125),
    ], curated_schema())

    assert compute_rfm(select_history(df, "2011-12-10"), "2011-12-10").first().Monetary == Decimal("0.25")


def test_compute_rfm_allows_zero_monetary_after_rounding(spark):  # Tổng tiền nhỏ hơn 0.005 làm tròn về 0.00 vẫn được chấp nhận (docstring cho phép), không bị loại.
    df = spark.createDataFrame([("a", "i1", utc(2011, 12, 1), 1, 0.004)], TX_COLUMNS)

    assert compute_rfm(select_history(df, "2011-12-10"), "2011-12-10").first().Monetary == Decimal("0.00")


# ---------------------------------------------------------------------------
# 5. ĐIỂM RFM: add_rfm_scores
# ---------------------------------------------------------------------------


def scores(spark, rows):  # Chạy add_rfm_scores và trả về dict CustomerID -> (R, F, M, mã RFM).
    return {r.CustomerID: (r.R_score, r.F_score, r.M_score, r.RFM_score)
            for r in add_rfm_scores(rfm_rows(spark, rows)).collect()}


def test_scores_direction_and_boundaries(spark):  # Chiều chấm điểm và các mốc thứ hạng: Recency thấp thì điểm R cao; Frequency/Monetary thấp thì điểm thấp.
    rows = [(str(i), 6 - i, i + 1, float(i + 1)) for i in range(6)]

    actual = scores(spark, rows)

    for i, expected in enumerate([1, 2, 3, 4, 5, 5]):
        assert actual[str(i)] == (expected, expected, expected, str(expected) * 3)


def test_scores_ties_and_id_independence(spark):  # Đồng hạng nhận cùng điểm; đổi tên ID và đảo thứ tự dòng không làm đổi điểm (CustomerID không ảnh hưởng).
    rows = [("a", 8, 1, 10.), ("b", 8, 1, 10.), ("c", 4, 3, 30.), ("d", 1, 5, 50.)]

    original = scores(spark, rows)
    renamed = scores(spark, [("new_" + r[0], *r[1:]) for r in reversed(rows)])

    assert original["a"] == original["b"]
    for key, value in original.items():
        assert renamed["new_" + key] == value


@pytest.mark.parametrize("count", [1, 4])
def test_scores_all_equal_get_one(spark, count):  # Một khách hoặc nhiều khách giống hệt nhau đều nhận 111 (rank 0, không có điểm trung tính).
    actual = scores(spark, [(str(i), 7, 2, 50.) for i in range(count)])

    assert set(actual.values()) == {(1, 1, 1, "111")}


def test_scores_only_constant_metric_gets_one(spark):  # Chỉ Frequency bằng nhau thì F_score = 1; R và M vẫn được chấm riêng.
    actual = scores(spark, [("a", 10, 1, 10.), ("b", 1, 1, 100.)])

    assert actual["a"] == (1, 1, 1, "111")
    assert actual["b"] == (5, 1, 5, "515")


def test_scores_keep_input_columns_and_add_four_new_ones(spark):  # add_rfm_scores giữ nguyên các cột đầu vào và chỉ thêm R_score, F_score, M_score, RFM_score.
    df = rfm_rows(spark, [("a", 1, 2, 5.), ("b", 3, 1, 2.)])

    result = add_rfm_scores(df)

    assert result.columns == df.columns + ["R_score", "F_score", "M_score", "RFM_score"]


# ---------------------------------------------------------------------------
# 6. STATE TÍCH LŨY
# ---------------------------------------------------------------------------


def test_customer_state_aggregates_history(spark):  # customer_state tính đúng lần mua cuối, số hóa đơn khác nhau và tổng tiền (dạng decimal) cho từng khách.
    state = {r.CustomerID: r for r in customer_state(history_sample(spark)).collect()}

    row = state["a"]
    assert row.Frequency == 3  # i1, i2, future
    assert row.MonetaryTotal == Decimal("1109.0000")  # 20 + 30 + 60 + 999
    last = customer_state(history_sample(spark)).select(
        F.date_format("LastPurchaseDate", "yyyy-MM-dd HH:mm:ss")).first()[0]
    assert last == "2011-12-10 00:00:00"  # lần mua cuối là dòng "future"


def test_state_to_rfm_computes_recency_and_rounds_money(spark):  # state_to_rfm chỉ tính Recency từ ngày chốt và làm tròn tiền HALF_UP, F/M lấy nguyên từ state.
    state = customer_state(tx_frame(spark, [
        ("A", "I1", datetime(2011, 12, 10, 9), 1, 0.005, "P1"),
    ]))

    row = state_to_rfm(state, "2011-12-12").first()

    assert (row.Recency, row.Frequency, row.Monetary) == (2, 1, Decimal("0.01"))
    assert row.run_date == date(2011, 12, 12)


def test_advance_state_without_previous_state_builds_initial_state(spark):  # Chưa có state cũ (lần đầu) thì state mới chính là customer_state của dữ liệu mới, kèm danh sách hóa đơn.
    novel = seed(spark)

    state, invoices = advance_state(None, None, novel)

    assert state.exceptAll(customer_state(novel)).isEmpty()
    assert invoices.count() == 2


def test_incremental_matches_full_recompute_split_invoice_and_rounding(spark):  # State cập nhật tăng dần phải cho cùng kết quả tính lại toàn bộ, kể cả hóa đơn có dòng đến ở 2 batch và làm tròn tổng 0.008 thành 0.01; cập nhật rỗng không đổi state.
    before, novel = seed(spark), extra_rows(spark)

    state, invoices = advance_state(
        customer_state(before), before.select("CustomerID", "InvoiceNo").distinct(), novel)
    result = state_to_rfm(state, "2011-12-12")
    expected = compute_rfm(before.unionByName(novel), "2011-12-12")

    assert result.exceptAll(expected).isEmpty()
    assert expected.exceptAll(result).isEmpty()
    a = result.where("CustomerID='A'").first()
    assert a.Frequency == 2 and a.Monetary == Decimal("0.01")
    assert invoices.where("CustomerID='A'").count() == 2
    same, _ = advance_state(state, invoices, novel.limit(0))
    assert same.exceptAll(state).isEmpty() and state.exceptAll(same).isEmpty()


def test_fingerprint_ignores_order_duplicates_and_partitioning_but_sees_changes(spark):  # Fingerprint của batch không đổi khi đổi thứ tự dòng/cột hay số partition, nhưng đổi khi nội dung đổi (dùng để phát hiện retry với dữ liệu khác).
    tx = canonical_transactions(seed(spark))

    same = tx.unionByName(tx).dropDuplicates().repartition(2).select(*reversed(tx.columns))
    changed = tx.withColumn("Quantity", F.col("Quantity") + 1)

    assert _fingerprint(tx) == _fingerprint(same)
    assert _fingerprint(tx) != _fingerprint(changed)
    assert _fingerprint(tx)["count"] == 2


# ---------------------------------------------------------------------------
# 7. ĐẶC TRƯNG VÀ MODEL
# ---------------------------------------------------------------------------


def test_preprocessing_log_and_vector_order(spark):  # prepare_kmeans_features áp log1p cho R/F/M; pipeline gom 3 cột *_log đúng thứ tự thành vector và dùng scaler withMean/withStd.
    prepared = prepare_kmeans_features(rfm_rows(spark, [("a", 9, 3, 0.), ("b", 1, 10, 50.)]))
    row = prepared.filter(F.col("CustomerID") == "a").first()

    assert [row[c] for c in LOG_FEATURES] == pytest.approx(
        [math.log1p(9), math.log1p(3), 0.])
    pipeline = kmeans_feature_pipeline()
    assembled = pipeline.getStages()[0].transform(prepared).filter(
        F.col("CustomerID") == "a").first()
    assert list(assembled.raw_features) == pytest.approx([row[c] for c in LOG_FEATURES])
    assert pipeline.getStages()[1].getWithMean() and pipeline.getStages()[1].getWithStd()


@pytest.mark.parametrize("metric", ["Recency", "Frequency", "Monetary"])
@pytest.mark.parametrize("bad", [None, -1., float("nan"), float("inf")])
def test_preprocessing_rejects_invalid_metrics(spark, metric, bad):  # Null, âm, NaN hoặc vô cực ở bất kỳ cột R/F/M nào đều bị từ chối.
    df = rfm_rows(spark, [("a", 1, 2, 5.)]).withColumn(metric, F.lit(bad).cast("double"))

    with pytest.raises(ValueError, match="Invalid RFM features"):
        prepare_kmeans_features(df)


@pytest.mark.parametrize("metric", ["Recency", "Frequency"])
def test_preprocessing_rejects_zero_recency_or_frequency(spark, metric):  # Recency và Frequency phải >= 1 (0 là dữ liệu vô lý); riêng Monetary = 0 thì được phép.
    df = rfm_rows(spark, [("a", 1, 2, 5.)]).withColumn(metric, F.lit(0))

    with pytest.raises(ValueError, match="Invalid RFM features"):
        prepare_kmeans_features(df)
    prepare_kmeans_features(rfm_rows(spark, [("a", 1, 2, 0.)]))  # Monetary = 0 hợp lệ


def test_preprocessing_rejects_empty_rfm(spark):  # Bảng RFM rỗng không được đưa vào chấm điểm.
    with pytest.raises(ValueError, match="Invalid RFM features"):
        prepare_kmeans_features(rfm_rows(spark, []))


def test_trained_pipeline_groups_similar_customers_together(spark, fitted_model):  # Model fit trên dữ liệu có 2 nhóm rõ rệt: hai khách giống nhau cùng cụm, hai nhóm khác cụm, và đủ 2 cụm 0/1; output chỉ thêm Cluster và model_version.
    df = rfm_rows(spark, BEST_AND_WORST)

    result_df = score_rfm_with_model(df, fitted_model, "v1")

    assert set(result_df.columns) - set(df.columns) == {"Cluster", "model_version"}
    result = {r.CustomerID: r.Cluster for r in result_df.collect()}
    assert result["best"] == result["best2"]
    assert result["worst"] == result["worst2"]
    assert result["best"] != result["worst"]
    assert set(result.values()) == {0, 1}


def test_score_rfm_with_model_drops_helper_columns_and_rejects_blank_version(spark, fitted_model):  # score_rfm_with_model bỏ các cột trung gian (log, vector) và từ chối model_version rỗng.
    df = rfm_rows(spark, [("a", 3, 19, 4900.)])

    result = score_rfm_with_model(df, fitted_model, "v9")

    assert not set(LOG_FEATURES) & set(result.columns)
    assert "raw_features" not in result.columns and "features" not in result.columns
    assert result.first().model_version == "v9"
    with pytest.raises(ValueError, match="must not be blank"):
        score_rfm_with_model(df, fitted_model, "  ")


def test_apply_rfm_model_adds_cluster_without_model_version(spark, fitted_model):  # apply_rfm_model (dùng cho DAG model-registry) chỉ thêm cột Cluster, KHÔNG thêm model_version (người gọi tự gắn), bỏ cột log/vector trung gian, và cụm đúng như dữ liệu 2 nhóm.
    df = rfm_rows(spark, BEST_AND_WORST)

    result = apply_rfm_model(df, fitted_model)

    assert set(result.columns) == set(df.columns) | {"Cluster"}
    clusters = {r.CustomerID: r.Cluster for r in result.collect()}
    assert clusters["best"] == clusters["best2"] and clusters["worst"] == clusters["worst2"]
    assert clusters["best"] != clusters["worst"]


def test_apply_rfm_model_still_validates_features(spark, fitted_model):  # apply_rfm_model dùng chung bước kiểm tra đặc trưng: Recency = 0 bị từ chối, không chấm điểm dữ liệu vô lý.
    bad = rfm_rows(spark, [("a", 0, 2, 5.0)])

    with pytest.raises(ValueError, match="Invalid RFM features"):
        apply_rfm_model(bad, fitted_model)


def test_default_kmeans_seed_is_fixed():  # Seed mặc định của KMeans cố định là 42 để huấn luyện lại cho kết quả tái lập được.
    assert DEFAULT_KMEANS_SEED == 42


def test_fit_rfm_model_delegates_to_shared_training_function(spark, monkeypatch):  # fit_rfm_model chỉ chuyển tiếp sang train_model của script training (một nơi duy nhất), truyền đúng k và seed (mặc định hoặc tùy chọn); dùng module giả nên không phụ thuộc script thật.
    calls = []
    fake = types.ModuleType("scripts.train_rfm_kmeans")
    fake.train_model = lambda rfm, k, seed: calls.append((k, seed)) or "model"
    monkeypatch.setitem(sys.modules, "scripts.train_rfm_kmeans", fake)
    df = rfm_rows(spark, [("a", 1, 2, 5.0)])

    assert fit_rfm_model(df, 3) == "model"
    assert fit_rfm_model(df, 4, seed=7) == "model"

    assert calls == [(3, DEFAULT_KMEANS_SEED), (4, 7)]


def test_daily_load_transform_never_retrains(spark, fitted_model):  # Daily chỉ load + transform: không fit lại Pipeline/Scaler/KMeans, không đổi mean/std/tâm cụm của model, và vẫn chạy được với 1 khách (ít hơn k cụm).
    daily = rfm_rows(spark, [("new", 3, 19, 4900.)])
    before_mean = list(fitted_model.stages[1].mean)
    before_std = list(fitted_model.stages[1].std)
    before_centers = [list(c) for c in fitted_model.stages[2].clusterCenters()]
    expected = fitted_model.transform(prepare_kmeans_features(daily)).first().Cluster

    with patch.object(PipelineModel, "load", return_value=fitted_model) as load, \
            patch.object(Pipeline, "fit", side_effect=AssertionError("retrained")), \
            patch("pyspark.ml.feature.StandardScaler.fit", side_effect=AssertionError("refit scaler")), \
            patch.object(KMeans, "fit", side_effect=AssertionError("refit KMeans")):
        result = add_rfm_segment(daily, "models/v1", "v1")
        row = result.first()
        load.assert_called_once_with("models/v1")

    assert row.Cluster == expected and row.model_version == "v1"
    assert set(result.columns) == set(daily.columns) | {"Cluster", "model_version"}
    assert list(fitted_model.stages[1].mean) == before_mean
    assert list(fitted_model.stages[1].std) == before_std
    assert [list(c) for c in fitted_model.stages[2].clusterCenters()] == before_centers


def test_saved_model_roundtrip_gives_same_predictions(spark, fitted_model, tmp_path):  # Lưu model ra đĩa rồi load_rfm_model: dự đoán, mean/std và K giống model gốc, và add_rfm_segment chấm điểm được từ đường dẫn thật.
    path = str(tmp_path / "v1")
    fitted_model.write().save(path)

    loaded = load_rfm_model(path)

    df = rfm_rows(spark, [("a", 1, 20, 5000.), ("b", 300, 1, 5.)])
    features = prepare_kmeans_features(df)
    assert [r.Cluster for r in loaded.transform(features).orderBy("CustomerID").collect()] == [
        r.Cluster for r in fitted_model.transform(features).orderBy("CustomerID").collect()]
    assert list(loaded.stages[1].mean) == list(fitted_model.stages[1].mean)
    assert list(loaded.stages[1].std) == list(fitted_model.stages[1].std)
    assert loaded.stages[-1].getK() == 2
    assert {r.model_version for r in add_rfm_segment(df, path, "v1").collect()} == {"v1"}


def test_load_rejects_wrong_stage_structure(fitted_model):  # Model không đúng cấu trúc 3 stage (rỗng, thiếu stage, sai thứ tự, thừa stage) bị từ chối với thông báo "must contain".
    assembler, scaler, kmeans = fitted_model.stages
    for stages in ([], [assembler, scaler], [scaler, assembler, kmeans],
                   [assembler, scaler, kmeans, kmeans]):
        with patch.object(PipelineModel, "load", return_value=PipelineModel(stages=stages)):
            with pytest.raises(ValueError, match="must contain"):
                load_rfm_model("invalid")


def kmeans_stub(real, k):  # Thay KMeansModel thật bằng bản giả có k tùy ý (Spark không cho tạo KMeans k < 2 thật); vẫn qua isinstance(KMeansModel) nhờ spec.
    stub = MagicMock(spec=KMeansModel)
    stub.getFeaturesCol.return_value = real.getFeaturesCol()
    stub.getPredictionCol.return_value = real.getPredictionCol()
    stub.getK.return_value = k
    return stub


@pytest.mark.parametrize("mutate", [
    lambda a, s, k: (a.copy({a.inputCols: list(reversed(LOG_FEATURES))}), s, k),
    lambda a, s, k: (a.copy({a.outputCol: "other"}), s, k),
    lambda a, s, k: (a, s.copy({s.inputCol: "other"}), k),
    lambda a, s, k: (a, s.copy({s.outputCol: "other"}), k),
    lambda a, s, k: (a, s.copy({s.withMean: False}), k),
    lambda a, s, k: (a, s.copy({s.withStd: False}), k),
    lambda a, s, k: (a, s, k.copy({k.featuresCol: "other"})),
    lambda a, s, k: (a, s, k.copy({k.predictionCol: "prediction"})),
    lambda a, s, k: (a, s, kmeans_stub(k, 1)),
], ids=["assembler_input_order", "assembler_output", "scaler_input", "scaler_output",
        "scaler_no_mean", "scaler_no_std", "kmeans_features_col",
        "kmeans_prediction_col", "kmeans_k_below_two"])
def test_load_rejects_contract_violations(fitted_model, mutate):  # Đúng loại stage nhưng sai cấu hình (thứ tự/ tên cột, scaler thiếu mean/std, KMeans sai cột hay k < 2) đều bị từ chối với thông báo "contract".
    wrong = PipelineModel(stages=list(mutate(*fitted_model.stages)))

    with patch.object(PipelineModel, "load", return_value=wrong):
        with pytest.raises(ValueError, match="contract"):
            load_rfm_model("wrong")


def test_load_accepts_the_shared_contract(fitted_model):  # Model đúng hợp đồng chung thì được trả về nguyên vẹn.
    with patch.object(PipelineModel, "load", return_value=fitted_model):
        assert load_rfm_model("ok") is fitted_model


@pytest.mark.parametrize("path,version", [("", "v1"), ("m", ""), ("m", " ")])
def test_daily_rejects_blank_model_configuration(spark, path, version):  # model_path hoặc model_version rỗng/khoảng trắng bị từ chối trước khi load model.
    with pytest.raises(ValueError, match="must not be blank"):
        add_rfm_segment(rfm_rows(spark, [("a", 1, 2, 5.)]), path, version)


def test_daily_requires_model_path_and_version(spark):  # add_rfm_segment bắt buộc truyền cả model_path và model_version (không có mặc định).
    df = rfm_rows(spark, [("a", 1, 2, 5.)])

    with pytest.raises(TypeError):
        add_rfm_segment(df)
    with pytest.raises(TypeError):
        add_rfm_segment(df, "m")


# ---------------------------------------------------------------------------
# 8. KIỂM TRA VÀ GHI OUTPUT: validate_rfm_output, write_rfm
# ---------------------------------------------------------------------------


def test_pipeline_output_passes_validation_and_rejects_wrong_cutoff_or_duplicates(spark, fitted_model):  # Chuỗi select_history -> compute_rfm -> scores -> model cho output hợp lệ; validator từ chối run_date khác ngày chốt và khách bị trùng.
    df = history_sample(spark).union(spark.createDataFrame(
        [("b", "j1", utc(2011, 12, 3), 2, 15.0)], TX_COLUMNS))
    validate_curated_contract(df)
    rfm = add_rfm_scores(compute_rfm(select_history(df, "2011-12-10"), "2011-12-10"))
    output = score_rfm_with_model(rfm, fitted_model, "v1")

    validate_rfm_output(output, "2011-12-10", k=2)

    row = output.filter(F.col("CustomerID") == "a").first()
    assert (row.Recency, row.Frequency, row.Monetary) == (1, 2, Decimal("110.00"))
    with pytest.raises(ValueError, match="run_date"):
        validate_rfm_output(output, "2011-12-11", k=2)
    with pytest.raises(ValueError, match="duplicate"):
        validate_rfm_output(output.union(output), "2011-12-10", k=2)


def test_validate_rfm_output_accepts_valid_output(valid_cluster_output):  # Output đúng mọi quy tắc được chấp nhận (kể cả khi chỉ rõ model_version mong đợi).
    validate_rfm_output(valid_cluster_output, "2011-12-10", k=2)
    validate_rfm_output(valid_cluster_output, "2011-12-10", k=2, model_version="v1")


def test_validate_rfm_output_rejects_missing_cluster(valid_cluster_output):  # Thiếu cột Cluster thì báo lỗi nêu tên cột thiếu.
    with pytest.raises(ValueError, match="missing columns.*Cluster"):
        validate_rfm_output(valid_cluster_output.drop("Cluster"), "2011-12-10", k=2)


def test_validate_rfm_output_rejects_empty_output(valid_cluster_output):  # Output rỗng bị từ chối (job daily phải tự xử lý trường hợp rỗng riêng).
    with pytest.raises(ValueError, match="empty"):
        validate_rfm_output(valid_cluster_output.limit(0), "2011-12-10", k=2)


@pytest.mark.parametrize("day", ["2011-12-1", "20111210", "2011-02-30"])
def test_validate_rfm_output_rejects_bad_run_date_format(valid_cluster_output, day):  # run_date truyền vào phải đúng định dạng YYYY-MM-DD và là ngày có thật.
    with pytest.raises(ValueError):
        validate_rfm_output(valid_cluster_output, day, k=2)


@pytest.mark.parametrize("cluster", [None, -1, 2, 999])
def test_validate_rfm_output_rejects_invalid_cluster(valid_cluster_output, cluster):  # Cluster null, âm, hoặc >= k đều bị từ chối.
    output = valid_cluster_output.withColumn("Cluster", F.lit(cluster).cast("int"))

    with pytest.raises(ValueError, match="invalid.*Cluster"):
        validate_rfm_output(output, "2011-12-10", k=2)


@pytest.mark.parametrize("cluster", [0.5, 0.0, "0", float("nan"), float("inf")])
def test_validate_rfm_output_rejects_noninteger_cluster_type(valid_cluster_output, cluster):  # Cluster phải có kiểu số nguyên; số thực, chuỗi, NaN, vô cực bị từ chối ngay cả khi giá trị trông hợp lệ.
    output = valid_cluster_output.withColumn("Cluster", F.lit(cluster))

    with pytest.raises(ValueError, match="Cluster must have an integer type"):
        validate_rfm_output(output, "2011-12-10", k=2)


@pytest.mark.parametrize("dtype", ["int", "bigint"])
@pytest.mark.parametrize("cluster", [0, 1])
def test_validate_rfm_output_accepts_cluster_boundaries(valid_cluster_output, dtype, cluster):  # Cluster 0 và k-1 hợp lệ ở cả hai kiểu nguyên int/bigint.
    output = valid_cluster_output.withColumn("Cluster", F.lit(cluster).cast(dtype))

    validate_rfm_output(output, "2011-12-10", k=2)


INVALID_OUTPUT_CASES = [  # (cột, giá trị làm hỏng, kiểu Spark): mỗi dòng là MỘT cách làm output sai.
    ("CustomerID", "", "string"), ("CustomerID", "   ", "string"), ("CustomerID", None, "string"),
    ("run_date", date(2011, 12, 9), "date"), ("run_date", None, "date"),
    ("LastPurchaseDate", None, "timestamp"),
    ("LastPurchaseDate", utc(2011, 12, 10), "timestamp"),
    ("LastPurchaseDate", utc(2011, 12, 11), "timestamp"),
    ("Recency", None, "int"), ("Recency", 0, "int"), ("Recency", 5, "int"),
    ("Frequency", None, "int"), ("Frequency", 0, "int"),
    ("Monetary", None, "double"), ("Monetary", -1.0, "double"),
    ("Monetary", float("nan"), "double"), ("Monetary", float("inf"), "double"),
    ("R_score", None, "int"), ("R_score", 0, "int"),
    ("F_score", 6, "int"), ("M_score", 0, "int"),
    ("RFM_score", None, "string"), ("RFM_score", "444", "string"),
]


@pytest.mark.parametrize("column,value,dtype", INVALID_OUTPUT_CASES,
                         ids=[f"{c}={v!r}" for c, v, _ in INVALID_OUTPUT_CASES])
def test_validate_rfm_output_rejects_each_invalid_metric(valid_cluster_output, column, value, dtype):  # Từng quy tắc của validator được kiểm riêng: ID rỗng, run_date sai, ngày mua cuối >= ngày chốt, Recency sai/ < 1, Frequency < 1, Monetary âm/NaN/vô cực, điểm ngoài 1..5, mã RFM không khớp.
    output = valid_cluster_output.withColumn(column, F.lit(value).cast(dtype))

    with pytest.raises(ValueError, match="invalid metrics, scores, or run_date"):
        validate_rfm_output(output, "2011-12-10", k=2)


def test_output_validation_requires_valid_k(valid_cluster_output, tmp_path):  # k là tham số bắt buộc (không có mặc định) và phải >= 2, ở cả validate_rfm_output lẫn write_rfm.
    with pytest.raises(TypeError):
        validate_rfm_output(valid_cluster_output, "2011-12-10")
    with pytest.raises(TypeError):
        write_rfm(valid_cluster_output, str(tmp_path), "2011-12-10")
    with pytest.raises(ValueError, match="at least 2"):
        validate_rfm_output(valid_cluster_output, "2011-12-10", k=1)


@pytest.mark.parametrize("version", [None, "", " ", 123])
def test_output_rejects_invalid_model_version(valid_cluster_output, version):  # model_version null, rỗng, khoảng trắng hoặc sai kiểu (số) trong output bị từ chối.
    output = valid_cluster_output.withColumn("model_version", F.lit(version))

    with pytest.raises(ValueError, match="model_version"):
        validate_rfm_output(output, "2011-12-10", k=2)


def test_output_requires_consistent_expected_model_version(valid_cluster_output):  # Thiếu cột model_version, khác version mong đợi, hoặc lẫn nhiều version trong cùng output đều bị từ chối.
    with pytest.raises(ValueError, match="missing columns.*model_version"):
        validate_rfm_output(valid_cluster_output.drop("model_version"), "2011-12-10", k=2)
    with pytest.raises(ValueError, match="model_version"):
        validate_rfm_output(valid_cluster_output, "2011-12-10", k=2, model_version="v2")
    mixed = valid_cluster_output.union(
        valid_cluster_output.withColumn("CustomerID", F.lit("b"))
        .withColumn("model_version", F.lit("v2")))
    with pytest.raises(ValueError, match="model_version"):
        validate_rfm_output(mixed, "2011-12-10", k=2)


def test_output_rejects_blank_expected_model_version(valid_cluster_output):  # Tham số model_version mong đợi bị rỗng cũng là cấu hình sai và bị từ chối.
    with pytest.raises(ValueError, match="must not be blank"):
        validate_rfm_output(valid_cluster_output, "2011-12-10", k=2, model_version=" ")


def test_write_rfm_rejects_invalid_cluster_before_writing(valid_cluster_output, tmp_path):  # Output sai thì write_rfm báo lỗi TRƯỚC khi ghi: không tạo thư mục output nào.
    output_root = tmp_path / "invalid_rfm"
    output = valid_cluster_output.withColumn("Cluster", F.lit(2))

    with pytest.raises(ValueError, match="invalid.*Cluster"):
        write_rfm(output, str(output_root), "2011-12-10", k=2)

    assert not output_root.exists()


def test_write_rfm_tolerates_trailing_slash_and_drops_run_date_column_from_files(spark, valid_cluster_output, tmp_path):  # Đường dẫn output có dấu "/" cuối vẫn ra đường dẫn partition đúng; cột run_date nằm ở tên thư mục (không lặp trong file) và đọc lại từ root thì có lại cột run_date.
    path = write_rfm(valid_cluster_output, str(tmp_path / "rfm") + "/", "2011-12-10", k=2)

    assert path == f"{tmp_path / 'rfm'}/run_date=2011-12-10"
    assert "run_date" not in spark.read.parquet(path).columns
    assert spark.read.parquet(str(tmp_path / "rfm")).first().run_date == date(2011, 12, 10)


def test_write_rfm_writes_partitioned_parquet_and_overwrites(spark, tmp_path, fitted_model):  # Ghi Parquet theo partition run_date=...; ghi lại cùng ngày thì THAY THẾ (không append).
    run_date, output_root = "2011-12-10", tmp_path / "rfm"
    rfm = add_rfm_scores(compute_rfm(select_history(
        history_sample(spark).union(spark.createDataFrame(
            [("b", "j1", utc(2011, 12, 3), 2, 15.0)], TX_COLUMNS)), run_date), run_date))
    output = score_rfm_with_model(rfm, fitted_model, "v1")

    output_path = write_rfm(output, str(output_root), run_date, k=2)

    partition_path = output_root / f"run_date={run_date}"
    assert partition_path == Path(output_path) and any(partition_path.glob("*.parquet"))
    replacement = output.filter(F.col("CustomerID") == "a").withColumn("CustomerID", F.lit("replacement"))
    write_rfm(replacement, str(output_root), run_date, k=2)
    rows = spark.read.parquet(str(output_root)).collect()
    assert [(r.CustomerID, r.run_date) for r in rows] == [("replacement", date(2011, 12, 10))]


def test_write_rfm_overwrite_preserves_other_dates(spark, tmp_path, fitted_model):  # Ghi đè partition của một ngày không động tới partition của ngày khác (nội dung ngày còn lại giữ nguyên).
    output_root = str(tmp_path / "rfm")
    transactions = history_sample(spark).union(spark.createDataFrame(
        [("b", "j1", utc(2011, 12, 3), 2, 15.0)], TX_COLUMNS))
    first_date, second_date = "2011-12-10", "2011-12-11"
    outputs = {d: score_rfm_with_model(add_rfm_scores(compute_rfm(
        select_history(transactions, d), d)), fitted_model, "v1")
        for d in (first_date, second_date)}
    write_rfm(outputs[first_date], output_root, first_date, k=2)
    write_rfm(outputs[second_date], output_root, second_date, k=2)
    before = spark.read.parquet(output_root).collect()
    assert len(before) == 4 and {r.run_date for r in before} == {date(2011, 12, 10), date(2011, 12, 11)}
    kept_before = sorted((r.asDict() for r in before if r.run_date == date(2011, 12, 11)),
                         key=lambda r: r["CustomerID"])

    replacement = outputs[first_date].filter(F.col("CustomerID") == "a").withColumn(
        "CustomerID", F.lit("replacement"))
    write_rfm(replacement, output_root, first_date, k=2)

    after = spark.read.parquet(output_root).collect()
    assert {(r.CustomerID, r.run_date) for r in after} == {
        ("replacement", date(2011, 12, 10)), ("a", date(2011, 12, 11)), ("b", date(2011, 12, 11))}
    kept_after = sorted((r.asDict() for r in after if r.run_date == date(2011, 12, 11)),
                        key=lambda r: r["CustomerID"])
    assert kept_after == kept_before


# ---------------------------------------------------------------------------
# 9. STORE: manifest và ingest_store
# ---------------------------------------------------------------------------


@pytest.fixture(params=["memory", "disk"])
def storage(request, spark, monkeypatch):  # Chạy mỗi test store 2 lần: "memory" thay RIÊNG phần I/O (đọc/ghi Parquet, JSON, rename) bằng bản giả trong RAM cho nhanh (phép biến đổi vẫn là Spark thật); "disk" dùng Hadoop/Parquet thật để kiểm tra cả rename nguyên tử.
    if request.param == "disk":
        return
    Path = spark._jvm.org.apache.hadoop.fs.Path
    frames, documents, files, directories = {}, {}, set(), set()

    class FS:
        def mkdirs(self, path):
            directories.add(str(path))
            return True

        def exists(self, path):
            return str(path) in files or str(path) in directories

        def createNewFile(self, path):
            if self.exists(path):
                return False
            files.add(str(path))
            return True

        def delete(self, path, recursive):
            files.discard(str(path))
            return True

        def listStatus(self, path):
            prefix = str(path) + "/"
            return [SimpleNamespace(getPath=lambda name=name: Path(name), isFile=lambda: True)
                    for name in files if name.startswith(prefix) and "/" not in name[len(prefix):]]

        def rename(self, source, target):
            source, target = str(source), str(target)
            if target in files:
                return False
            documents[target] = documents.pop(source)
            files.remove(source)
            files.add(target)
            return True

    fs = FS()

    def partition(writer, *cols):
        writer._rfm_partition_cols = cols
        return writer

    def write(writer, path, *args, **kwargs):
        name = str(Path(path))
        data = writer._df  # materialize để cắt lineage như ghi/đọc thật
        if getattr(writer, "_rfm_partition_cols", None):
            for row in data.select("transaction_date").distinct().collect():
                part = data.where(F.col("transaction_date") == F.lit(row[0])).drop("transaction_date")
                frames[name + "/transaction_date=" + str(row[0])] = spark.createDataFrame(
                    part.collect(), part.schema)
        else:
            frames[name] = spark.createDataFrame(data.collect(), data.schema)
        directories.add(name)
        files.add(name + "/_SUCCESS")

    def read(reader, *paths, **kwargs):
        result = frames[str(Path(paths[0]))]
        for path in paths[1:]:
            result = result.unionByName(frames[str(Path(path))])
        return result

    def write_json(fs_, path, obj):
        import copy
        documents[str(path)] = copy.deepcopy(obj)
        files.add(str(path))

    monkeypatch.setattr(job, "_store_filesystem", lambda session, path: (fs, Path(path)))
    monkeypatch.setattr(job, "_read_json", lambda session, fs_, path: documents[str(path)])
    monkeypatch.setattr(job, "_write_json", write_json)
    monkeypatch.setattr(DataFrameWriter, "partitionBy", partition)
    monkeypatch.setattr(DataFrameWriter, "parquet", write)
    monkeypatch.setattr(DataFrameReader, "parquet", read)


def test_store_real_roundtrip_retry_overlap_and_snapshot(spark, tmp_path, storage):  # Luồng store đầy đủ: khởi tạo, cập nhật, retry cùng batch-id trả cùng version, batch trùng hoàn toàn không cộng thêm tiền, RFM từ store khớp tính lại toàn bộ, đọc snapshot cũ theo ngày chốt, chọn khách hoạt động, và chặn batch-id xung đột/ingest lùi ngày.
    first, batch, store = [str(tmp_path / p) for p in ("first", "batch", "store")]
    seed(spark).write.parquet(first)
    initial = ingest_store(spark, first, store, "seed", "2011-12-11", True, 2)
    extra = extra_rows(spark)
    seed(spark).unionByName(extra).write.parquet(batch)

    updated = ingest_store(spark, batch, store, "day11", "2011-12-12", False, 2)

    assert updated["version"] != initial["version"]
    assert ingest_store(spark, batch, store, "day11", "2011-12-12", False, 2)["version"] == updated["version"]
    ingest_store(spark, batch, store, "overlap", "2011-12-12", False, 2)  # batch trùng, ID khác
    actual, _ = read_store_rfm(spark, store, "2011-12-12")
    expected = compute_rfm(seed(spark).unionByName(extra), "2011-12-12")
    assert actual.exceptAll(expected).isEmpty() and expected.exceptAll(actual).isEmpty()
    old, _ = read_store_rfm(spark, store, "2011-12-11")
    assert old.count() == 2 and old.where("CustomerID='A'").first().Frequency == 1
    population = daily_rfm_population(spark, updated, "2011-12-12")
    assert {r.CustomerID for r in population.collect()} == {"A", "B", "C"}
    with pytest.raises(ValueError, match="Conflicting"):
        ingest_store(spark, first, store, "day11", "2011-12-12", False, 2)
    with pytest.raises(ValueError, match="backwards"):
        ingest_store(spark, first, store, "back", "2011-12-11", False, 2)


def test_store_only_updates_new_date_and_guards(spark, tmp_path, storage):  # Chỉ ghi lại partition ngày mới (ngày cũ được tham chiếu lại, không ghi lại); null ở cột phụ so sánh null-safe; chặn input sau ngày chốt, đổi số bucket, khởi tạo lại; writer thứ hai không xóa được lock của writer đang chạy.
    source, batch, future, store = [str(tmp_path / p) for p in ("source", "new", "future", "store")]
    seed(spark).write.parquet(source)
    delta = tx_frame(spark, [("A", "I3", datetime(2011, 12, 11, 9), 1, 1.0, None)])
    delta.unionByName(delta).write.parquet(batch)
    tx_frame(spark, [("A", "I4", datetime(2011, 12, 12), 1, 1.0, "P")]).write.parquet(future)
    with pytest.raises(ValueError, match="First ingestion"):
        ingest_store(spark, source, store, "seed", "2011-12-11", False, 1)
    initial = ingest_store(spark, source, store, "seed", "2011-12-11", True, 1)

    updated = ingest_store(spark, batch, store, "new", "2011-12-12", False, 1)

    assert (initial["buckets"]["0"]["transactions"]["2011-12-10"]
            == updated["buckets"]["0"]["transactions"]["2011-12-10"])
    ingest_store(spark, batch, store, "new_replay", "2011-12-12", False, 1)  # null StockCode phải so sánh null-safe
    rfm, _ = read_store_rfm(spark, store, "2011-12-12")
    assert rfm.where("CustomerID='A'").first().Frequency == 2
    assert rfm.where("CustomerID='A'").first().Monetary == Decimal("1.00")
    for kwargs, pattern in [({"input_path": future}, "on/after"), ({"num_buckets": 2}, "Bucket count"),
                            ({"initialize": True}, "already initialized")]:
        opts = dict(input_path=batch, store_path=store, batch_id="bad", run_date="2011-12-12",
                    initialize=False, num_buckets=1)
        opts.update(kwargs)
        with pytest.raises(ValueError, match=pattern):
            ingest_store(spark, **opts)
    fs, root = job._store_filesystem(spark, store)
    lock = spark._jvm.org.apache.hadoop.fs.Path(root, "_writer.lock")
    assert fs.createNewFile(lock)
    try:
        with pytest.raises(ValueError, match="locked"):
            ingest_store(spark, batch, store, "locked", "2011-12-12", False, 1)
        assert fs.exists(lock)  # writer thứ hai không được xóa lock của người khác
    finally:
        fs.delete(lock, False)


def test_store_failed_write_not_published(spark, tmp_path, storage):  # Ghi lỗi giữa chừng thì KHÔNG công bố manifest, lock được nhả, và retry cùng batch thành công; lỗi ở lần cập nhật sau không làm đổi phiên bản đang công bố.
    source, store = str(tmp_path / "source"), str(tmp_path / "store")
    seed(spark).write.parquet(source)
    with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", side_effect=RuntimeError("write failed")):
        with pytest.raises(RuntimeError, match="write failed"):
            ingest_store(spark, source, store, "seed", "2011-12-11", True, 1)
    with pytest.raises(ValueError, match="No committed"):
        load_manifest(spark, store)

    initial = ingest_store(spark, source, store, "seed", "2011-12-11", True, 1)

    assert initial["version"] == "000000000001"
    with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", side_effect=RuntimeError("write failed")):
        with pytest.raises(RuntimeError, match="write failed"):
            ingest_store(spark, source, store, "failed_update", "2011-12-12", False, 1)
    assert load_manifest(spark, store)["version"] == initial["version"]


def test_store_infers_existing_bucket_count(spark, tmp_path, storage):  # Không truyền num_buckets ở lần cập nhật sau thì tự dùng số bucket đã lưu trong store.
    first, store = [str(tmp_path / p) for p in ("seed", "store")]
    seed(spark).write.parquet(first)

    initialized = ingest_store(spark, first, store, "seed", "2011-12-11", True, 2)
    updated = ingest_store(spark, first, store, "next", "2011-12-12", False, None)

    assert initialized["num_buckets"] == updated["num_buckets"] == 2
    assert read_store_table(spark, updated, "state").count() == 2


def test_daily_rfm_population_scores_every_customer_including_idle(spark, tmp_path, storage):  # daily_rfm_population chấm điểm TOÀN BỘ khách trong state (kể cả khách không mua hôm nay như B): F/M cộng dồn, chỉ Recency tính lại theo ngày chốt, và khớp kết quả tính lại từ toàn bộ giao dịch.
    first, batch, store = [str(tmp_path / p) for p in ("first", "batch", "store")]
    seed(spark).write.parquet(first)
    seed(spark).unionByName(extra_rows(spark)).write.parquet(batch)
    ingest_store(spark, first, store, "seed", "2011-12-11", True, 2)
    manifest = ingest_store(spark, batch, store, "day11", "2011-12-12", False, 2)

    population = daily_rfm_population(spark, manifest, "2011-12-12")

    expected = add_rfm_scores(compute_rfm(seed(spark).unionByName(extra_rows(spark)), "2011-12-12"))
    assert population.exceptAll(expected).isEmpty() and expected.exceptAll(population).isEmpty()
    rows = {r.CustomerID: r for r in population.collect()}
    assert set(rows) == {"A", "B", "C"}
    assert rows["B"].Recency == 3 and rows["B"].Frequency == 1  # B không mua từ 12/09 nhưng vẫn được chấm điểm
    assert {"R_score", "F_score", "M_score", "RFM_score"} <= set(population.columns)


def test_store_defaults_to_standard_bucket_count_on_first_ingest(spark, tmp_path, storage):  # Lần khởi tạo đầu mà không chỉ định num_buckets thì dùng DEFAULT_NUM_BUCKETS.
    first, store = [str(tmp_path / p) for p in ("seed", "store")]
    seed(spark).write.parquet(first)

    manifest = ingest_store(spark, first, store, "seed", "2011-12-11", True, None)

    assert manifest["num_buckets"] == DEFAULT_NUM_BUCKETS


def test_store_rejects_changed_business_schema(spark, tmp_path, storage):  # Batch sau có schema nghiệp vụ khác store (thiếu StockCode) thì bị từ chối, không ghi gì.
    first, changed, store = [str(tmp_path / p) for p in ("seed", "changed", "store")]
    seed(spark).write.parquet(first)
    seed(spark).drop("StockCode").write.parquet(changed)
    ingest_store(spark, first, store, "seed", "2011-12-11", True, 1)

    with pytest.raises(ValueError, match="business schema changed"):
        ingest_store(spark, changed, store, "next", "2011-12-12", False, 1)

    assert load_manifest(spark, store)["version"] == "000000000001"


@pytest.mark.parametrize("kwargs,pattern", [
    ({"batch_id": "../x"}, "Invalid batch-id"),
    ({"batch_id": ""}, "Invalid batch-id"),
    ({"batch_id": None}, "Invalid batch-id"),
    ({"num_buckets": 0}, "Invalid bucket count"),
    ({"num_buckets": 4097}, "Invalid bucket count"),
    ({"run_date": "20111212"}, "Invalid run-date"),
])
def test_ingest_store_validates_arguments_before_touching_storage(spark, tmp_path, kwargs, pattern):  # Tham số sai (batch-id, số bucket ngoài 1..4096, run-date không chuẩn) bị chặn ngay từ đầu, chưa tạo store hay lock nào.
    opts = dict(input_path=str(tmp_path / "in"), store_path=str(tmp_path / "store"),
                batch_id="b1", run_date="2011-12-12", initialize=True, num_buckets=1)
    opts.update(kwargs)

    with pytest.raises(ValueError, match=pattern):
        ingest_store(spark, **opts)

    assert not (tmp_path / "store").exists()


def test_ingest_store_rejects_input_inside_store(spark, tmp_path):  # Input nằm trong thư mục store (hoặc ngược lại) bị chặn trước khi tạo lock, tránh job tự ghi đè dữ liệu nguồn.
    store = tmp_path / "store"

    with pytest.raises(ValueError, match="separate and non-overlapping"):
        ingest_store(spark, str(store / "input"), str(store), "b1", "2011-12-12", True, 1)

    assert not store.exists()


def test_load_manifest_selects_snapshot_by_cutoff_and_version(spark, tmp_path, storage):  # load_manifest chọn snapshot mới nhất không vượt ngày chốt, hoặc đúng version khi retry; version vượt ngày chốt hay không tồn tại bị từ chối; chưa có snapshot nào đủ cũ thì báo lỗi.
    first, batch, store = [str(tmp_path / p) for p in ("first", "batch", "store")]
    seed(spark).write.parquet(first)
    seed(spark).unionByName(extra_rows(spark)).write.parquet(batch)
    v1 = ingest_store(spark, first, store, "seed", "2011-12-11", True, 1)["version"]
    v2 = ingest_store(spark, batch, store, "day11", "2011-12-12", False, 1)["version"]

    assert load_manifest(spark, store)["version"] == v2
    assert load_manifest(spark, store, "2011-12-12")["version"] == v2
    assert load_manifest(spark, store, "2011-12-11")["version"] == v1
    assert load_manifest(spark, store, version=v1)["version"] == v1
    with pytest.raises(ValueError, match="beyond requested run-date"):
        load_manifest(spark, store, "2011-12-11", version=v2)
    with pytest.raises(ValueError, match="No committed"):
        load_manifest(spark, store, "2011-12-10")
    with pytest.raises(ValueError, match="No committed"):
        load_manifest(spark, store, version="000000000099")


def test_manifest_listing_ignores_unrelated_files(spark, tmp_path):  # Chỉ file đúng dạng 12 chữ số + .json trong _commits mới tính là manifest; file tạm/ghi chú/tên lạ bị bỏ qua (chạy trên đĩa thật).
    first, store = str(tmp_path / "first"), tmp_path / "store"
    seed(spark).write.parquet(first)
    initial = ingest_store(spark, first, str(store), "seed", "2011-12-11", True, 1)
    commits = store / "_commits"
    (commits / "notes.txt").write_text("x")
    (commits / "1.json").write_text("{}")
    (commits / "_pending_abc.json").write_text("{}")
    (commits / "000000000099.json.tmp").write_text("{}")

    assert load_manifest(spark, str(store))["version"] == initial["version"]


def test_read_store_table_reads_state_invoices_and_transactions(spark, tmp_path):  # read_store_table đọc đủ 3 bảng theo manifest: state (1 dòng/khách), invoices, và transactions hợp nhất các partition ngày (chạy trên đĩa thật).
    first, store = str(tmp_path / "first"), str(tmp_path / "store")
    seed(spark).write.parquet(first)
    manifest = ingest_store(spark, first, store, "seed", "2011-12-11", True, 2)

    assert read_store_table(spark, manifest, "state").count() == 2
    assert read_store_table(spark, manifest, "invoices").count() == 2
    transactions = read_store_table(spark, manifest, "transactions")
    assert transactions.count() == 2
    assert {"CustomerID", "InvoiceNo", "StockCode"} <= set(transactions.columns)


def test_read_store_table_rejects_empty_store(spark):  # Manifest không có bucket nào thì báo "Empty store" thay vì trả bảng rỗng.
    with pytest.raises(ValueError, match="Empty store"):
        read_store_table(spark, {"buckets": {}}, "state")


def test_publish_manifest_refuses_to_overwrite_existing_version(spark, tmp_path):  # Công bố manifest cho version đã tồn tại thì báo lỗi, không ghi đè phiên bản đã commit.
    fs, root = job._store_filesystem(spark, str(tmp_path / "store"))
    fs.mkdirs(root)
    manifest = {"version": "000000000001", "run_date": "2011-12-11"}
    job._publish_store_manifest(spark, fs, root, manifest)

    with pytest.raises(ValueError, match="Atomic manifest publication failed"):
        job._publish_store_manifest(spark, fs, root, manifest)


def test_store_rejects_bucket_write_without_success_marker(spark, tmp_path):  # Nếu Parquet ghi xong mà thiếu _SUCCESS thì job báo "write did not complete", không công bố manifest và nhả lock (mô phỏng bằng cách tắt marker trên đĩa thật).
    first, store = str(tmp_path / "first"), str(tmp_path / "store")
    seed(spark).write.parquet(first)
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    try:
        with pytest.raises(ValueError, match="did not complete"):
            ingest_store(spark, first, store, "seed", "2011-12-11", True, 1)
    finally:
        spark.conf.unset("mapreduce.fileoutputcommitter.marksuccessfuljobs")

    assert not (tmp_path / "store" / "_commits").exists() or not list((tmp_path / "store" / "_commits").glob("*.json"))
    assert not (tmp_path / "store" / "_writer.lock").exists()


# ---------------------------------------------------------------------------
# 10. MAIN: job daily end-to-end
# ---------------------------------------------------------------------------


def patch_main_runtime(spark, monkeypatch, argv):  # Chuẩn bị job.main(): parse argv bằng parse_args thật, trả SparkSession test thay cho builder, và biến spark.stop() thành bộ đếm (để không làm chết session dùng chung).
    args = parse_args(argv)

    class Builder:
        def appName(self, *_):
            return self

        def config(self, *_):
            return self

        def getOrCreate(self):
            return spark

    stops = []
    monkeypatch.setattr(job, "parse_args", lambda: args)
    monkeypatch.setattr(job, "SparkSession", SimpleNamespace(builder=Builder()))
    monkeypatch.setattr(spark, "stop", lambda: stops.append(1))
    return stops


def test_main_stops_spark_and_reraises_when_a_step_fails(spark, monkeypatch):  # Một bước lỗi (ingest) thì lỗi được ném lên để Airflow đánh dấu task FAILED, và Spark vẫn được stop (khối finally).
    stops = patch_main_runtime(spark, monkeypatch, DAILY)
    monkeypatch.setattr(job, "ingest_store", lambda *a: (_ for _ in ()).throw(RuntimeError("ingest failed")))

    with pytest.raises(RuntimeError, match="ingest failed"):
        job.main()

    assert stops == [1]


def test_main_rejects_overlapping_store_and_output(spark, monkeypatch, tmp_path):  # Output trùng/lồng với store bị chặn trước khi ingest (không ghi gì), và Spark vẫn được stop.
    shared = str(tmp_path / "shared")
    stops = patch_main_runtime(spark, monkeypatch, DAILY + [
        "--store-path", shared, "--output", shared + "/rfm", "--model-version", "v1"])
    monkeypatch.setattr(job, "ingest_store", lambda *a: pytest.fail("must fail before ingest"))

    with pytest.raises(ValueError, match="separate and non-overlapping"):
        job.main()

    assert stops == [1]


@pytest.fixture(scope="module")
def saved_model(fitted_model, tmp_path_factory):  # Lưu fitted_model ra đĩa một lần để job main() load bằng đường dẫn thật.
    path = str(tmp_path_factory.mktemp("models") / "v1")
    fitted_model.write().save(path)
    return path


@pytest.fixture
def daily_workspace(spark, tmp_path, saved_model):  # Dựng thư mục input/store/output riêng, ghi sẵn batch giao dịch của 4 khách (a: 2 hóa đơn, b, c, d) trước ngày chốt 2011-12-12, và trả về bộ tham số daily.
    rows = [
        ("a", "A1", datetime(2011, 12, 5, 9), 2, 10.0, "P1"),
        ("a", "A2", datetime(2011, 12, 11, 9), 1, 30.0, "P1"),   # hoạt động trong kỳ
        ("b", "B1", datetime(2011, 12, 1, 9), 3, 10.0, "P2"),    # không hoạt động
        ("c", "C1", datetime(2011, 12, 10, 9), 1, 8.0, "P3"),    # không hoạt động (trước 12/11)
        ("d", "D1", datetime(2011, 12, 11, 23, 59), 1, 5.0, "P4"),  # hoạt động trong kỳ
    ]
    tx_frame(spark, rows).write.parquet(str(tmp_path / "input"))
    argv = ["--input", str(tmp_path / "input"), "--batch-id", "seed", "--initialize-store",
            "--store-path", str(tmp_path / "store"), "--output", str(tmp_path / "out"),
            "--run-date", "2011-12-12", "--model-path", saved_model, "--model-version", "v1"]
    return SimpleNamespace(argv=argv, out=str(tmp_path / "out"), store=str(tmp_path / "store"))


def test_main_end_to_end_scores_every_customer_and_retry_is_idempotent(spark, monkeypatch, daily_workspace, caplog):  # Job daily chạy thật end-to-end: luôn chấm TOÀN BỘ khách (kể cả b, c không mua trong ngày) và ghi đủ 4 khách đúng R/F/M, Cluster thuộc [0,2), model_version đúng, cảnh báo dân số nhỏ; chạy lại cùng batch (retry) cho kết quả y hệt và không nhân đôi.
    stops = patch_main_runtime(spark, monkeypatch, daily_workspace.argv)

    with caplog.at_level(logging.WARNING, logger="ecommerce_rfm"):
        job.main()
        job.main()  # retry cùng batch-id

    rows = {r.CustomerID: r for r in spark.read.parquet(daily_workspace.out).collect()}
    assert set(rows) == {"a", "b", "c", "d"}
    assert (rows["a"].Recency, rows["a"].Frequency, rows["a"].Monetary) == (1, 2, Decimal("50.00"))
    assert (rows["b"].Recency, rows["b"].Frequency, rows["b"].Monetary) == (11, 1, Decimal("30.00"))
    assert (rows["c"].Recency, rows["c"].Monetary) == (2, Decimal("8.00"))
    assert (rows["d"].Recency, rows["d"].Frequency, rows["d"].Monetary) == (1, 1, Decimal("5.00"))
    assert {r.Cluster for r in rows.values()} <= {0, 1}
    assert {r.model_version for r in rows.values()} == {"v1"}
    assert all(r.run_date == date(2011, 12, 12) for r in rows.values())
    assert any("Only 4 customers found" in m for m in caplog.messages)
    assert load_manifest(spark, daily_workspace.store)["version"] == "000000000001"
    assert stops == [1, 1]


def test_pipeline_context_flag_delegates_to_pipeline_main(monkeypatch):  # Cờ --pipeline-context (điểm tích hợp Airflow) chuyển sang retail_pipeline.pipeline_main("rfm") thay vì chạy main() thường.
    called = {}
    fake = types.ModuleType("retail_pipeline")
    fake.pipeline_main = lambda step: called.setdefault("step", step)
    monkeypatch.setitem(sys.modules, "retail_pipeline", fake)
    monkeypatch.setattr(sys, "argv", ["pyspark_rfm.py", "--pipeline-context"])

    runpy.run_module("scripts.pyspark_rfm", run_name="__main__")

    assert called["step"] == "rfm"
