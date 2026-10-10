"""Tests for curated input, RFM metrics, relative scores, and CLI arguments."""
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import math

import pytest


pytest.importorskip("pyspark") # nếu thiếu PySpark, pytest bỏ qua module. Skip không có nghĩa đã kiểm tra thành công

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from scripts.pyspark_rfm import (  # noqa: E402
    add_rfm_scores,
    add_rfm_segment,
    compute_rfm,
    parse_args,
    select_history,
    validate_curated_contract,
    validate_rfm_output,
    write_rfm,
    LOG_FEATURES,
    prepare_kmeans_features,
    kmeans_feature_pipeline,
    load_rfm_model,
)
from scripts.train_rfm_kmeans import train_model, parse_args as training_args
from scripts.pyspark_rfm import (
    normalize_transaction_rows, _assert_transaction_schema,
    assert_separate_paths,
)

def nullable_schema():
    """Schema cho phép null - dùng riêng khi cần test giá trị null,
    vì curated_schema() mặc định nullable=False sẽ bị Spark chặn ngay
    lúc tạo DataFrame, chưa kịp tới validate_curated_contract."""
    return StructType(
        [
            StructField("CustomerID", StringType(), True),
            StructField("InvoiceNo", StringType(), True),
            StructField("InvoiceDate", TimestampType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("UnitPrice", DoubleType(), True),
        ]
    )

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-pyspark-rfm")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def curated_schema(customer_id_type=None): # Hàm phụ trợ tạo schema chuẩn
    return StructType(
        [
            StructField(
                "CustomerID", customer_id_type or StringType(), False
            ),
            StructField("InvoiceNo", StringType(), False),
            StructField("InvoiceDate", TimestampType(), False),
            StructField("Quantity", IntegerType(), False),
            StructField("UnitPrice", DoubleType(), False),
        ]
    )


def score_with_trained_model(rfm, k=2):
    """Real training/transform; file save/load is checked separately."""
    model = train_model(rfm, k=k)
    with patch("scripts.pyspark_rfm.PipelineModel.load", return_value=model):
        return add_rfm_segment(rfm, "test-model-v1", "v1")


# 1. Test validate_curated_contract - check input
# Valid data must be accepted
def test_validate_curated_contract_accepts_canonical_data(spark):
    df = spark.createDataFrame(
        [("17850", "536365", datetime(2026, 9, 1), 6, 2.55)],
        curated_schema(),
    )

    validate_curated_contract(df)

# A numeric customer identifier violates the canonical string schema (CustomerID kiểu số bị từ chối)
def test_validate_curated_contract_rejects_noncanonical_schema(spark):
    df = spark.createDataFrame(
        [(17850, "536365", datetime(2026, 9, 1), 6, 2.55)],
        curated_schema(IntegerType()),
    )

    with pytest.raises(ValueError, match="CustomerID.*string"):
        validate_curated_contract(df)


def test_validate_curated_contract_rejects_missing_required_columns(spark):
    df = spark.createDataFrame(
        [("17850", "536365", datetime(2026, 9, 1), 6, 2.55)],
        curated_schema(),
    ).drop("UnitPrice")

    with pytest.raises(ValueError, match="missing required columns.*UnitPrice"):
        validate_curated_contract(df)


# Zero quantity must be rejected.
def test_validate_curated_contract_rejects_unclean_values(spark):
    df = spark.createDataFrame(
        [("17850", "536365", datetime(2026, 9, 1), 0, 2.55)],
        curated_schema(),
    )

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)

# Input rỗng (0 dòng) phải bị chặn
def test_validate_curated_contract_rejects_empty_input(spark):
    df = spark.createDataFrame([], curated_schema())

    with pytest.raises(ValueError, match="empty"):
        validate_curated_contract(df)

# Null ở từng cột bắt buộc, test riêng từng cột để biết chính xác cột nào gây lỗi.
@pytest.mark.parametrize(
    "row",
    [
        (None, "536365", datetime(2026, 9, 1), 6, 2.55),
        ("17850", None, datetime(2026, 9, 1), 6, 2.55),
        ("17850", "536365", None, 6, 2.55),
        ("17850", "536365", datetime(2026, 9, 1), None, 2.55),
        ("17850", "536365", datetime(2026, 9, 1), 6, None),
    ],
)
def test_validate_curated_contract_rejects_null_values(spark, row):
    df = spark.createDataFrame([row], nullable_schema())

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)

# CustomerID/InvoiceNo chỉ toàn khoảng trắng phải bị coi là rỗng, không phải chuỗi hợp lệ.
@pytest.mark.parametrize(
    "row",
    [
        ("   ", "536365", datetime(2026, 9, 1), 6, 2.55),
        ("17850", "   ", datetime(2026, 9, 1), 6, 2.55),
    ],
)
def test_validate_curated_contract_rejects_blank_ids(spark, row):
    df = spark.createDataFrame([row], curated_schema())

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)


# Quantity âm, UnitPrice = 0, UnitPrice âm - đều phải bị chặn riêng biệt.
@pytest.mark.parametrize(
    "quantity,price",
    [(-1, 2.55), (6, 0.0), (6, -2.55)],
    ids=["negative_quantity", "zero_price", "negative_price"],
)
def test_validate_curated_contract_rejects_negative_or_zero_values(spark, quantity, price):
    df = spark.createDataFrame(
        [("17850", "536365", datetime(2026, 9, 1), quantity, price)],
        curated_schema(),
    )

    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(df)


# 2. Test add_rfm_scores
def scores(spark, rows):
    df = spark.createDataFrame(
        rows, "CustomerID string, Recency int, Frequency int, Monetary double"
    )
    return {r.CustomerID: (r.R_score, r.F_score, r.M_score, r.RFM_score)
            for r in add_rfm_scores(df).collect()}

# Kiểm tra chiều chấm điểm và các mốc thứ hạng: R thấp -> điểm cao, F/M thấp -> điểm thấp
def test_direction_and_boundaries(spark):
    rows = [(str(i), 6-i, i+1, float(i+1)) for i in range(6)]
    actual = scores(spark, rows)
    for i, expected in enumerate([1, 2, 3, 4, 5, 5]):
        assert actual[str(i)] == (expected, expected, expected, str(expected)*3)

# Đồng hạng nhận cùng điểm; dữ liệu đổi tên ID và đảo thứ tự dòng vẫn giữ kết quả kỳ vọng
def test_ties_and_id_independence(spark):
    # (CustomerID, Recency, Frequency, Monetary)
    rows = [("a", 8, 1, 10.), ("b", 8, 1, 10.),
            ("c", 4, 3, 30.), ("d", 1, 5, 50.)]
    original = scores(spark, rows)
    renamed = scores(spark, [("new_"+r[0], *r[1:]) for r in reversed(rows)])
    assert original["a"] == original["b"]
    for key, value in original.items():
        assert renamed["new_"+key] == value


@pytest.mark.parametrize("count", [1, 4])
# Một khách or bốn khách giống nhau nhận 111
def test_all_equal_uses_rank_zero(spark, count):
    actual = scores(spark, [(str(i), 7, 2, 50.) for i in range(count)])
    assert set(actual.values()) == {(1, 1, 1, "111")}

# Chỉ Frequency bằng nhau thì F_score =1; R/M vẫn chấm riêng?
def test_only_constant_metric_gets_one(spark):
    actual = scores(spark, [("a", 10, 1, 10.), ("b", 1, 1, 100.)])
    assert actual["a"] == (1, 1, 1, "111")
    assert actual["b"] == (5, 1, 5, "515")


# Daily scoring thêm Cluster và model_version; không gán tên business.
# (id thô do KMeans gán) - KHÔNG xếp hạng, KHÔNG gán tên business.
# Việc so sánh giá trị giữa các cụm và đặt tên nhóm là bước làm tay, sau
# khi EDA trên dữ liệu cụm thật (không tự động hoá trong hàm này).

def rfm_rows(spark, rows):
    """rows: list of (CustomerID, Recency, Frequency, Monetary)."""
    return spark.createDataFrame(
        rows,
        "CustomerID string, Recency int, Frequency int, Monetary double",
    )


# k không còn giá trị mặc định: gọi thiếu --k/k phải báo lỗi rõ ràng.
def test_train_model_requires_explicit_k(spark):
    df = rfm_rows(spark, [(str(i), 10, i + 1, float(i + 1)) for i in range(4)])
    with pytest.raises(TypeError):
        train_model(df)


def test_train_model_rejects_k_below_two(spark):
    df = rfm_rows(spark, [(str(i), 10, i + 1, float(i + 1)) for i in range(4)])
    with pytest.raises(ValueError, match="at least 2"):
        train_model(df, k=1)


def test_train_model_rejects_k_larger_than_customer_count(spark):
    df = rfm_rows(spark, [("a", 10, 1, 10.0), ("b", 5, 2, 20.0)])
    with pytest.raises(ValueError, match="exceeds the number of customers"):
        train_model(df, k=3)


@pytest.mark.parametrize("distinct_vectors", [1, 2])
def test_train_model_rejects_insufficient_distinct_features(spark, distinct_vectors):
    df = rfm_rows(spark, [
        (str(i), 10, 1 + i % distinct_vectors, 50.0)
        for i in range(6)
    ])
    with pytest.raises(ValueError, match="distinct feature vectors"):
        train_model(df, k=3)


def test_train_model_rejects_fewer_occupied_clusters(spark, monkeypatch):
    from pyspark.ml import Pipeline

    class CollapsedModel:
        def transform(self, prepared):
            return prepared.withColumn("Cluster", F.lit(0))

    monkeypatch.setattr(Pipeline, "fit", lambda self, dataset: CollapsedModel())
    df = rfm_rows(spark, [("a", 1, 20, 5000.0), ("b", 300, 1, 5.0)])
    with pytest.raises(ValueError, match="1 occupied clusters; expected 2"):
        train_model(df, k=2)


# Output thêm Cluster và model_version. Không có ClusterRank, không
# có Segment - việc xếp hạng/đặt tên để làm tay ở bước EDA riêng.
# Vẫn kiểm tra tính chất phân cụm cơ bản: 2 khách rất giống nhau (gần,
# mua nhiều, chi nhiều) phải rơi vào cùng 1 Cluster, và khác hẳn với 2
# khách rất khác biệt (xa, mua ít, chi ít).
def test_add_rfm_segment_groups_similar_customers_together(spark):
    rows = [
        ("best", 1, 20, 5000.0),
        ("best2", 2, 22, 4800.0),
        ("worst", 300, 1, 5.0),
        ("worst2", 310, 1, 8.0),
    ]
    df = rfm_rows(spark, rows)
    result_df = score_with_trained_model(df, k=2)

    assert set(result_df.columns) - set(df.columns) == {"Cluster", "model_version"}
    assert "ClusterRank" not in result_df.columns
    assert "Segment" not in result_df.columns

    result = {r.CustomerID: r.Cluster for r in result_df.collect()}
    assert result["best"] == result["best2"]
    assert result["worst"] == result["worst2"]
    assert result["best"] != result["worst"]
    assert set(result.values()) == {0, 1}  # raw KMeans ids for k=2 are 0-indexed


# 3. Test history selection and RFM metric computation.
def sample(spark):
    return spark.createDataFrame([
        ("a", "i1", datetime(2011, 12, 1, tzinfo=timezone.utc), 2, 10.),
        ("a", "i1", datetime(2011, 12, 1, tzinfo=timezone.utc), 1, 30.),
        ("a", "i2", datetime(2011, 12, 9, tzinfo=timezone.utc), 3, 20.),
        ("a", "future", datetime(2011, 12, 10, tzinfo=timezone.utc), 1, 999.),
    ], "CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double")


# sample() chỉ có 1 khách ("a") -> không đủ để chạy K-means (cần k <= số
# khách). Các test liên quan tới add_rfm_segment/write_rfm dùng bản mở rộng
# này (thêm khách "b") thay vì sample() thuần.
def sample_two_customers(spark):
    return sample(spark).union(
        spark.createDataFrame(
            [("b", "j1", datetime(2011, 12, 3, tzinfo=timezone.utc), 2, 15.0)],
            "CustomerID string, InvoiceNo string, InvoiceDate timestamp, "
            "Quantity int, UnitPrice double",
        )
    )


# Không có giao dịch nào trước ngày chốt -> select_history phải raise, không trả về bảng rỗng.
def test_select_history_rejects_no_transactions_before_cutoff(spark):
    df = spark.createDataFrame(
        [("17850", "536365", datetime(2026, 9, 5, tzinfo=timezone.utc), 6, 2.55)],
        "CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double",
    )

    with pytest.raises(ValueError, match="No transactions"):
        select_history(df, "2026-09-01")


# Ranh giới ngày chốt: giao dịch ngay trước nửa đêm phải được giữ,
# giao dịch đúng vào 00:00:00 ngày chốt phải bị loại.
def test_select_history_boundary_exact_cutoff(spark):
    df = spark.createDataFrame(
        [
            ("a", "i1", datetime(2011, 12, 9, 23, 59, 59, tzinfo=timezone.utc), 1, 10.),
            ("a", "i2", datetime(2011, 12, 10, 0, 0, 0, tzinfo=timezone.utc), 1, 20.),
        ],
        "CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double",
    )

    history = select_history(df, "2011-12-10")
    invoices = [row.InvoiceNo for row in history.collect()]
    assert invoices == ["i1"]

# Khi có cột year/month (dữ liệu curated partitioned), select_history phải
# loại đúng partition không liên quan trước, kết quả cuối vẫn giống hệt như
# không có year/month (chỉ khác cách Spark đọc dữ liệu, không khác kết quả).
def test_select_history_prunes_partitions_when_year_month_present(spark):
    df = spark.createDataFrame(
        [
            ("a", "i1", datetime(2011, 8, 15, tzinfo=timezone.utc), 1, 10., 2011, 8),
            ("a", "i2", datetime(2011, 9, 5, tzinfo=timezone.utc), 1, 20., 2011, 9),
            # Nằm trong tháng cutoff nhưng sau ngày cutoff -> vẫn phải bị loại
            # bởi filter InvoiceDate, dù year/month khớp.
            ("a", "i3", datetime(2011, 9, 20, tzinfo=timezone.utc), 1, 999., 2011, 9),
            # Partition ở tương lai -> phải bị loại bởi cả 2 lớp filter.
            ("a", "i4", datetime(2012, 1, 1, tzinfo=timezone.utc), 1, 999., 2012, 1),
        ],
        "CustomerID string, InvoiceNo string, InvoiceDate timestamp, "
        "Quantity int, UnitPrice double, year int, month int",
    )

    history = select_history(df, "2011-09-10")
    invoices = {row.InvoiceNo for row in history.collect()}
    assert invoices == {"i1", "i2"}


# Không có year/month thì hành vi giữ nguyên như trước (chỉ lọc InvoiceDate).
def test_select_history_without_partition_columns_still_filters_by_date(spark):
    history = select_history(sample(spark), "2011-12-10")
    invoices = {row.InvoiceNo for row in history.collect()}
    assert invoices == {"i1", "i2"}


# Nhiều khách hàng cùng lúc: đảm bảo groupBy tách đúng theo từng CustomerID,
# không bị lẫn Frequency/Monetary giữa các khách.
def test_compute_rfm_separates_multiple_customers(spark):
    df = spark.createDataFrame(
        [
            ("a", "i1", datetime(2011, 12, 1, tzinfo=timezone.utc), 2, 10.),
            ("b", "i2", datetime(2011, 12, 5, tzinfo=timezone.utc), 1, 50.),
            ("b", "i3", datetime(2011, 12, 8, tzinfo=timezone.utc), 3, 20.),
        ],
        "CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double",
    )

    rfm = compute_rfm(select_history(df, "2011-12-10"), "2011-12-10").collect()
    result = {
        row.CustomerID: (row.Recency, row.Frequency, row.Monetary)
        for row in rfm
    }

    assert len(rfm) == 2
    assert result == {
        "a": (9, 1, Decimal("20.00")),
        "b": (2, 2, Decimal("110.00")),
    }


# Giá có phần thập phân, cố tình chọn số ra đúng 3 chữ số thập phân (x.xx5)
# để kiểm tra Monetary làm tròn đúng kiểu HALF_UP (12.345 -> 12.35, không phải 12.34).
def test_compute_rfm_rounds_monetary_half_up(spark):
    df = spark.createDataFrame(
        [("a", "i1", datetime(2011, 12, 1, tzinfo=timezone.utc), 1, 12.345)],
        "CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double",
    )

    rfm = compute_rfm(select_history(df, "2011-12-10"), "2011-12-10").first()
    assert rfm.Monetary == Decimal("12.35")


def test_compute_rfm_rounds_after_summing_line_totals(spark):
    """Sum first: 0.125 + 0.125 = 0.25, not 0.13 + 0.13 = 0.26."""
    df = spark.createDataFrame(
        [
            ("a", "i1", datetime(2011, 12, 1, tzinfo=timezone.utc), 1, 0.125),
            ("a", "i2", datetime(2011, 12, 2, tzinfo=timezone.utc), 1, 0.125),
        ],
        curated_schema(),
    )
    rfm = compute_rfm(select_history(df, "2011-12-10"), "2011-12-10")
    assert rfm.first().Monetary == Decimal("0.25")


# Tính R,F,M; loại giao dịch tại ngày chốt, từ chối ngày output sai và khách trùng
def test_metrics_and_cutoff(spark):
    df = sample_two_customers(spark)
    validate_curated_contract(df)
    output = add_rfm_scores(compute_rfm(select_history(df, "2011-12-10"), "2011-12-10"))
    output = score_with_trained_model(output, k=2)
    validate_rfm_output(output, "2011-12-10", k=2)
    row = output.filter(F.col("CustomerID") == "a").first()
    assert (row.Recency, row.Frequency, row.Monetary) == (1, 2, Decimal("110.00"))
    with pytest.raises(ValueError, match="run_date"):
        validate_rfm_output(output, "2011-12-11", k=2)
    with pytest.raises(ValueError, match="duplicate"):
        validate_rfm_output(output.union(output), "2011-12-10", k=2)

# 4. Test partitioned Parquet output and overwrite behavior.
@pytest.fixture
def valid_cluster_output(spark):
    # Validator tests do not need to train a clustering model.
    return spark.createDataFrame(
        [("a", date(2011, 12, 10), datetime(2011, 12, 9, tzinfo=timezone.utc),
          1, 2, 110.0, 5, 5, 5, "555", 0)],
        "CustomerID string, run_date date, LastPurchaseDate timestamp, "
        "Recency int, Frequency int, Monetary double, R_score int, "
        "F_score int, M_score int, RFM_score string, Cluster int",
    ).withColumn("model_version", F.lit("v1"))


def test_validate_rfm_output_rejects_missing_cluster(valid_cluster_output):
    with pytest.raises(ValueError, match="missing columns.*Cluster"):
        validate_rfm_output(valid_cluster_output.drop("Cluster"), "2011-12-10", k=2)


@pytest.mark.parametrize("cluster", [None, -1, 2, 999])
def test_validate_rfm_output_rejects_invalid_cluster(valid_cluster_output, cluster):
    output = valid_cluster_output.withColumn("Cluster", F.lit(cluster).cast("int"))
    with pytest.raises(ValueError, match="invalid.*Cluster"):
        validate_rfm_output(output, "2011-12-10", k=2)


@pytest.mark.parametrize("cluster", [0.5, 0.0, "0", float("nan"), float("inf")])
def test_validate_rfm_output_rejects_noninteger_cluster_type(valid_cluster_output, cluster):
    output = valid_cluster_output.withColumn("Cluster", F.lit(cluster))
    with pytest.raises(ValueError, match="Cluster must have an integer type"):
        validate_rfm_output(output, "2011-12-10", k=2)


@pytest.mark.parametrize("dtype", ["int", "bigint"])
@pytest.mark.parametrize("cluster", [0, 1])
def test_validate_rfm_output_accepts_cluster_boundaries(valid_cluster_output, dtype, cluster):
    output = valid_cluster_output.withColumn("Cluster", F.lit(cluster).cast(dtype))
    validate_rfm_output(output, "2011-12-10", k=2)


def test_output_validation_requires_valid_k(valid_cluster_output, tmp_path):
    with pytest.raises(TypeError):
        validate_rfm_output(valid_cluster_output, "2011-12-10")
    with pytest.raises(TypeError):
        write_rfm(valid_cluster_output, str(tmp_path), "2011-12-10")
    with pytest.raises(ValueError, match="at least 2"):
        validate_rfm_output(valid_cluster_output, "2011-12-10", k=1)


def test_write_rfm_rejects_invalid_cluster_before_writing(valid_cluster_output, tmp_path):
    output_root = tmp_path / "invalid_rfm"
    output = valid_cluster_output.withColumn("Cluster", F.lit(2))
    with pytest.raises(ValueError, match="invalid.*Cluster"):
        write_rfm(output, str(output_root), "2011-12-10", k=2)
    assert not output_root.exists()


def test_write_rfm_writes_partitioned_parquet_and_overwrites(spark, tmp_path):
    run_date = "2011-12-10"
    output_root = tmp_path / "rfm"
    output = add_rfm_scores(
        compute_rfm(select_history(sample_two_customers(spark), run_date), run_date)
    )
    output = score_with_trained_model(output, k=2)
    output_path = write_rfm(output, str(output_root), run_date, k=2)
    partition_path = output_root / f"run_date={run_date}"

    assert partition_path == Path(output_path)
    assert any(partition_path.glob("*.parquet"))

    # Replace the whole partition with a single renamed row (overwrite, not append).
    replacement = (
        output.filter(F.col("CustomerID") == "a")
        .withColumn("CustomerID", F.lit("replacement"))
    )
    write_rfm(replacement, str(output_root), run_date, k=2)

    rows = spark.read.parquet(str(output_root)).collect()
    assert [(row.CustomerID, row.run_date) for row in rows] == [
        ("replacement", date(2011, 12, 10))
    ]


def test_write_rfm_overwrite_preserves_other_dates(spark, tmp_path):
    """Replacing one daily partition must preserve all values in the other."""
    output_root = str(tmp_path / "rfm")
    transactions = sample_two_customers(spark)
    first_date, second_date = "2011-12-10", "2011-12-11"
    first_output = add_rfm_scores(
        compute_rfm(select_history(transactions, first_date), first_date)
    )
    second_output = add_rfm_scores(
        compute_rfm(select_history(transactions, second_date), second_date)
    )
    first_output = score_with_trained_model(first_output, k=2)
    second_output = score_with_trained_model(second_output, k=2)

    write_rfm(first_output, output_root, first_date, k=2)
    write_rfm(second_output, output_root, second_date, k=2)

    before = spark.read.parquet(output_root).collect()
    # 2 khách ("a", "b") x 2 ngày = 4 dòng.
    assert len(before) == 4
    assert {row.run_date for row in before} == {
        date(2011, 12, 10), date(2011, 12, 11),
    }
    preserved_before = sorted(
        (row.asDict() for row in before if row.run_date == date(2011, 12, 11)),
        key=lambda r: r["CustomerID"],
    )

    # Chỉ đổi tên khách "a" của ngày đầu để overwrite đúng 1 partition,
    # tránh 2 dòng cùng ngày bị trùng CustomerID.
    replacement = (
        first_output.filter(F.col("CustomerID") == "a")
        .withColumn("CustomerID", F.lit("replacement"))
    )
    write_rfm(replacement, output_root, first_date, k=2)

    after = spark.read.parquet(output_root).collect()
    assert len(after) == 3
    assert {(row.CustomerID, row.run_date) for row in after} == {
        ("replacement", date(2011, 12, 10)),
        ("a", date(2011, 12, 11)),
        ("b", date(2011, 12, 11)),
    }
    preserved_after = sorted(
        (row.asDict() for row in after if row.run_date == date(2011, 12, 11)),
        key=lambda r: r["CustomerID"],
    )
    assert preserved_after == preserved_before
    replaced = [row for row in after if row.run_date == date(2011, 12, 10)]
    assert replaced[0].asDict() == replacement.first().asDict()


@pytest.mark.parametrize("price", [float("nan"), float("inf"), -float("inf")])
# Reject NaN, infinite
def test_nonfinite_price_rejected(spark, price):
    with pytest.raises(ValueError, match="cleaning job"):
        validate_curated_contract(sample(spark).withColumn("UnitPrice", F.lit(price)))

# Nhận ngày hợp lệ, từ chối ngày không tồn tại
def test_cli_date():
    args = parse_args(
        ["--input", "new", "--batch-id", "b1"] + ["--store-path", "store", "--output", "out", "--run-date", "2011-12-10", "--model-path", "models/v1", "--model-version", "v1"]
    )
    assert args.run_date == "2011-12-10"
    assert args.model_path == "models/v1"
    assert args.model_version == "v1"
    with pytest.raises(SystemExit):
        parse_args(
            ["--input", "new", "--batch-id", "b1"] + ["--store-path", "store", "--output", "out", "--run-date", "2011-02-30", "--model-path", "models/v1", "--model-version", "v1"]
        )


# k remains explicit in the training CLI, not in daily scoring.
def test_cli_k_is_required_and_validated():
    with pytest.raises(SystemExit):
        training_args(["--store-path", "store", "--run-date", "2011-12-10",
                       "--model-output", "models/v1"])
    with pytest.raises(SystemExit):
        training_args(["--store-path", "store", "--run-date", "2011-12-10",
                       "--model-output", "models/v1", "--k", "1"])


@pytest.fixture(scope="module")
def fitted_model(spark):
    df = rfm_rows(spark, [
        ("a", 1, 20, 5000.), ("b", 2, 22, 4800.),
        ("c", 300, 1, 5.), ("d", 310, 1, 8.),
    ])
    return train_model(df, k=2, seed=42)


def test_preprocessing_log_and_vector_order(spark):
    df = rfm_rows(spark, [("a", 9, 3, 0.), ("b", 1, 10, 50.)])
    prepared = prepare_kmeans_features(df)
    row = prepared.filter(F.col("CustomerID") == "a").first()
    assert [row[c] for c in LOG_FEATURES] == pytest.approx(
        [math.log1p(9), math.log1p(3), 0.])
    pipeline = kmeans_feature_pipeline()
    assembled = pipeline.getStages()[0].transform(prepared).filter(
        F.col("CustomerID") == "a").first()
    assert list(assembled.raw_features) == pytest.approx([row[c] for c in LOG_FEATURES])
    assert pipeline.getStages()[1].getWithMean()
    assert pipeline.getStages()[1].getWithStd()


@pytest.mark.parametrize("metric", ["Recency", "Frequency", "Monetary"])
@pytest.mark.parametrize("bad", [None, -1., float("nan"), float("inf")])
def test_preprocessing_rejects_invalid_metrics(spark, metric, bad):
    df = rfm_rows(spark, [("a", 1, 2, 5.)]).withColumn(metric, F.lit(bad).cast("double"))
    with pytest.raises(ValueError, match="Invalid RFM features"):
        prepare_kmeans_features(df)


def test_preprocessing_rejects_empty_rfm(spark):
    with pytest.raises(ValueError, match="Invalid RFM features"):
        prepare_kmeans_features(rfm_rows(spark, []))


def test_daily_load_transform_never_retrains(spark, fitted_model):
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import StandardScaler

    # Daily may have one customer and occupy fewer than k clusters.
    daily = rfm_rows(spark, [("new", 3, 19, 4900.)])
    before_mean = list(fitted_model.stages[1].mean)
    before_std = list(fitted_model.stages[1].std)
    before_centers = [list(c) for c in fitted_model.stages[2].clusterCenters()]
    expected = fitted_model.transform(prepare_kmeans_features(daily)).first().Cluster
    with patch.object(PipelineModel, "load", return_value=fitted_model) as load, \
         patch.object(Pipeline, "fit", side_effect=AssertionError("retrained")), \
         patch.object(StandardScaler, "fit", side_effect=AssertionError("refit scaler")), \
         patch.object(KMeans, "fit", side_effect=AssertionError("refit KMeans")):
        result = add_rfm_segment(daily, "models/v1", "v1")
        row = result.first()
        load.assert_called_once_with("models/v1")
    assert row.Cluster == expected
    assert row.model_version == "v1"
    assert set(result.columns) == set(daily.columns) | {"Cluster", "model_version"}
    assert list(fitted_model.stages[1].mean) == before_mean
    assert list(fitted_model.stages[1].std) == before_std
    assert [list(c) for c in fitted_model.stages[2].clusterCenters()] == before_centers


def test_training_save_load_predictions_and_immutable_path(spark, fitted_model, tmp_path):
    from pyspark.ml import PipelineModel
    from pyspark.ml.clustering import KMeansModel
    from pyspark.ml.feature import StandardScalerModel

    path = str(tmp_path / "v1")
    assert isinstance(fitted_model.stages[1], StandardScalerModel)
    assert isinstance(fitted_model.stages[2], KMeansModel)
    fitted_model.write().save(path)
    loaded = load_rfm_model(path)
    df = rfm_rows(spark, [("a", 1, 20, 5000.), ("b", 300, 1, 5.)])
    features = prepare_kmeans_features(df)
    assert [r.Cluster for r in loaded.transform(features).orderBy("CustomerID").collect()] == [
        r.Cluster for r in fitted_model.transform(features).orderBy("CustomerID").collect()]
    assert list(loaded.stages[1].mean) == list(fitted_model.stages[1].mean)
    assert list(loaded.stages[1].std) == list(fitted_model.stages[1].std)
    assert PipelineModel.load(path).stages[-1].getK() == 2
    scored = add_rfm_segment(df, path, "v1")
    assert {r.model_version for r in scored.collect()} == {"v1"}
    with pytest.raises(Exception, match="(?i)already exists"):
        fitted_model.write().save(path)


def test_load_rejects_incompatible_pipeline(fitted_model):
    from pyspark.ml import PipelineModel
    with patch.object(PipelineModel, "load", return_value=PipelineModel(stages=[])):
        with pytest.raises(ValueError, match="must contain"):
            load_rfm_model("invalid")
    wrong = PipelineModel(stages=[*fitted_model.stages])
    # Copy assembler to avoid mutating the shared fitted model.
    wrong.stages[0] = fitted_model.stages[0].copy({}).setInputCols(list(reversed(LOG_FEATURES)))
    with patch.object(PipelineModel, "load", return_value=wrong):
        with pytest.raises(ValueError, match="contract"):
            load_rfm_model("wrong-order")


@pytest.mark.parametrize("path,version", [("", "v1"), ("m", ""), ("m", " ")])
def test_daily_rejects_blank_model_configuration(spark, path, version):
    with pytest.raises(ValueError, match="must not be blank"):
        add_rfm_segment(rfm_rows(spark, [("a", 1, 2, 5.)]), path, version)


def test_daily_requires_model_path_and_version(spark):
    df = rfm_rows(spark, [("a", 1, 2, 5.)])
    with pytest.raises(TypeError):
        add_rfm_segment(df)
    with pytest.raises(TypeError):
        add_rfm_segment(df, "m")


@pytest.mark.parametrize("version", [None, "", " ", 123])
def test_output_rejects_invalid_model_version(valid_cluster_output, version):
    with pytest.raises(ValueError, match="model_version"):
        validate_rfm_output(valid_cluster_output.withColumn("model_version", F.lit(version)),
                            "2011-12-10", k=2)


def test_output_requires_consistent_expected_model_version(valid_cluster_output):
    with pytest.raises(ValueError, match="missing columns.*model_version"):
        validate_rfm_output(valid_cluster_output.drop("model_version"), "2011-12-10", k=2)
    with pytest.raises(ValueError, match="model_version"):
        validate_rfm_output(valid_cluster_output, "2011-12-10", k=2, model_version="v2")
    mixed = valid_cluster_output.union(valid_cluster_output.withColumn("CustomerID", F.lit("b"))
                                      .withColumn("model_version", F.lit("v2")))
    with pytest.raises(ValueError, match="model_version"):
        validate_rfm_output(mixed, "2011-12-10", k=2)


def test_daily_cli_requires_model_and_rejects_legacy_k():
    base = ["--store-path", "store", "--output", "out", "--run-date", "2011-12-10"]
    for extra in [[], ["--model-path", " "], ["--model-version", "v1"],
                  ["--model-path", "m", "--model-version", "v1", "--k", "2"],
                  ["--model-path", "m", "--model-version", " "]]:
        with pytest.raises(SystemExit):
            parse_args(["--input", "new", "--batch-id", "b1"] + base + extra)


def test_training_cli_date_seed_and_output():
    base = ["--store-path", "store", "--model-output", "models/v1", "--k", "4"]
    args = training_args(base + ["--run-date", "2011-12-10", "--seed", "7"])
    assert (args.k, args.seed, args.model_output) == (4, 7, "models/v1")
    for day in ["2011-02-30", "20111210"]:
        with pytest.raises(SystemExit):
            training_args(base + ["--run-date", day])


# Cumulative transactions: all prior coverage remains above.
def batch_rows(spark, rows):
    return spark.createDataFrame(rows, "CustomerID string, InvoiceNo string, "
                                 "InvoiceDate timestamp, Quantity int, UnitPrice double, StockCode string")


def seed_rows(spark):
    return batch_rows(spark, [
        ("a", "old", datetime(2026, 9, 30, tzinfo=timezone.utc), 1, 100., "p1"),
        ("inactive", "i0", datetime(2026, 9, 20, tzinfo=timezone.utc), 1, 50., "p0"),
    ])


def new_rows(spark):
    return batch_rows(spark, [
        ("a", "old", datetime(2026, 9, 30, tzinfo=timezone.utc), 1, 100., "p1"),
        ("a", "new", datetime(2026, 10, 3, tzinfo=timezone.utc), 2, 10., "p2"),
        ("a", "new", datetime(2026, 10, 3, tzinfo=timezone.utc), 2, 10., "p3"),
    ])


def test_merge_cumulative_rfm_overlap_and_distinct_invoice_lines(spark):
    incoming = new_rows(spark)
    # An exact repeated row must disappear; a different product line must stay.
    repeated = incoming.union(incoming.filter(F.col("StockCode") == "p2"))
    merged = normalize_transaction_rows(seed_rows(spark).unionByName(repeated))
    assert merged.count() == 4
    rows = {r.CustomerID: r for r in compute_rfm(select_history(merged, "2026-10-04"),
                                                "2026-10-04").collect()}
    assert (rows["a"].Recency, rows["a"].Frequency, rows["a"].Monetary) == (1, 2, Decimal("140.00"))
    assert (rows["inactive"].Recency, rows["inactive"].Frequency,
            rows["inactive"].Monetary) == (14, 1, Decimal("50.00"))


def test_normalization_rebuilds_partition_dates(spark):
    incoming = seed_rows(spark).withColumn("year", F.lit(2099)).withColumn("month", F.lit(12))
    normalized = normalize_transaction_rows(incoming)
    assert {(r.year, r.month) for r in normalized.collect()} == {(2026, 9)}
    assert select_history(normalized, "2026-10-01").count() == 2


def test_merge_schema_drift_and_reserved_column_rejected(spark):
    with pytest.raises(ValueError, match="schemas must match"):
        _assert_transaction_schema(seed_rows(spark), new_rows(spark).drop("StockCode"))
    with pytest.raises(ValueError, match="reserved"):
        normalize_transaction_rows(seed_rows(spark).withColumn("batch_id", F.lit("b")))




# Incremental: trạng thái, retry, khách hoạt động và lưu trữ.
from scripts.pyspark_rfm import (
    canonical_transactions, customer_state, state_to_rfm, advance_state,
    active_customers, ingest_store, read_store_rfm, read_store_active,
    load_manifest, _fingerprint, read_store_table,
)
from scripts.pyspark_rfm import compute_rfm, parse_args
train_args = training_args




def frame(spark, rows):
    rows = [tuple(value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) else value for value in row) for row in rows]
    return spark.createDataFrame(rows, 'CustomerID string, InvoiceNo string, InvoiceDate timestamp, Quantity int, UnitPrice double, StockCode string')


def seed(spark):
    return frame(spark, [('A', 'I1', datetime(2011,12,10,9), 1, 0.004, 'P1'),
                         ('B', 'I2', datetime(2011,12,9,9), 2, 5.0, 'P2')])


def test_incremental_matches_full_recompute_split_invoice_and_rounding(spark):
    before = seed(spark)
    novel = frame(spark, [('A','I1',datetime(2011,12,10,9),1,0.004,'P3'),
                          ('A','I3',datetime(2011,12,11,9),1,0.004,'P1'),
                          ('C','I4',datetime(2011,12,11,9),1,20.0,'P1')])
    state, invoices = advance_state(customer_state(before), before.select('CustomerID','InvoiceNo').distinct(), novel)
    result = state_to_rfm(state, '2011-12-12')
    expected = compute_rfm(before.unionByName(novel), '2011-12-12')
    assert result.exceptAll(expected).isEmpty()
    assert expected.exceptAll(result).isEmpty()
    a = result.where("CustomerID='A'").first()
    assert a.Frequency == 2 and a.Monetary == Decimal('0.01')
    assert invoices.where("CustomerID='A'").count() == 2
    # A no-op update must leave state unchanged (including inactive B).
    same, _ = advance_state(state, invoices, novel.limit(0))
    assert same.exceptAll(state).isEmpty() and state.exceptAll(same).isEmpty()


def test_active_period_excludes_inactive_and_boundaries(spark):
    tx = frame(spark, [('A','1',datetime(2011,12,10,23,59),1,1.0,'P'),
                      ('B','2',datetime(2011,12,11),1,1.0,'P'),
                      ('C','3',datetime(2011,12,12),1,1.0,'P')])
    assert [r.CustomerID for r in active_customers(tx,'2011-12-11','2011-12-12').collect()] == ['B']
    assert active_customers(tx,'2011-12-13','2011-12-14').isEmpty()


def test_fingerprint_order_duplicate_and_schema(spark):
    tx = canonical_transactions(seed(spark))
    assert _fingerprint(tx) == _fingerprint(tx.unionByName(tx).dropDuplicates().repartition(2).select(*reversed(tx.columns)))
    changed = tx.withColumn('Quantity',F.col('Quantity')+1)
    assert _fingerprint(tx) != _fingerprint(changed)


def test_incremental_cli():
    base = ['--output','out','--model-path','model','--model-version','v1','--run-date','2011-12-12']
    args = parse_args(["--input", "new", "--batch-id", "b1"] + base + ['--store-path','store'])
    assert args.period_start == '2011-12-11' and not args.all_customers
    args = parse_args(["--input", "new", "--batch-id", "b1"] + base + ['--store-path','store','--input','new','--batch-id','b1','--period-start','2011-12-01'])
    assert args.period_start == '2011-12-01'
    for extra in [['--store-path','store','--batch-id',''],
                  ['--store-path','store','--initialize-store','--input',''],
                  ['--store-path','store','--history-path','old'],
                  ['--store-path','store','--period-start','2011-12-12'],
                  ['--store-path','store','--num-buckets','0']]:
        with pytest.raises(SystemExit):
            parse_args(["--input", "new", "--batch-id", "b1"] + base+extra)
    assert train_args(['--store-path','store','--run-date','2012-01-01','--k','2','--model-output','m']).input is None


def test_daily_empty_period_writes_schema_without_loading_model(spark,monkeypatch):
    from types import SimpleNamespace
    from pyspark.sql.readwriter import DataFrameWriter
    import scripts.pyspark_rfm as job
    population = compute_rfm(seed(spark),'2011-12-12')
    args = parse_args(["--input", "new", "--batch-id", "b1"] + ['--store-path','store','--output','out','--model-path','model',
                       '--model-version','v1','--run-date','2011-12-12'])
    class Builder:
        def appName(self,*args): return self
        def config(self,*args): return self
        def getOrCreate(self): return spark
    writes = []
    monkeypatch.setattr(job,'parse_args',lambda:args)
    monkeypatch.setattr(job,'SparkSession',SimpleNamespace(builder=Builder()))
    monkeypatch.setattr(spark,'stop',lambda:None)
    monkeypatch.setattr(job,'ingest_store',lambda *args:{'version':'1'})
    monkeypatch.setattr(job,'read_store_table',lambda *args:population)
    monkeypatch.setattr(job,'state_to_rfm',lambda *args:population)
    monkeypatch.setattr(job,'read_store_active',lambda *args:population.select('CustomerID').limit(0))
    monkeypatch.setattr(job,'load_rfm_model',lambda *args:pytest.fail('Empty period must not load model'))
    monkeypatch.setattr(DataFrameWriter,'parquet',lambda writer,path:writes.append((path,writer._df)))
    job.main()
    assert writes[0][0] == 'out/run_date=2011-12-12'
    assert writes[0][1].isEmpty()
    types = dict(writes[0][1].dtypes)
    assert types['Cluster'] == 'int' and types['model_version'] == 'string'


@pytest.fixture(params=['memory', 'disk'])
def storage(request, spark, monkeypatch):
    """Memory mode replaces storage I/O only; transformations are real Spark.
    Disk mode additionally verifies real Hadoop/Parquet and rename semantics.
    """
    if request.param == 'disk':
        return
    from types import SimpleNamespace
    from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
    import scripts.pyspark_rfm as module
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
            prefix = str(path) + '/'
            return [SimpleNamespace(getPath=lambda name=name: Path(name), isFile=lambda: True)
                    for name in files if name.startswith(prefix) and '/' not in name[len(prefix):]]
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
        # Materialize to break lineage just as a real write/read does.
        data = writer._df
        if getattr(writer, '_rfm_partition_cols', None):
            for row in data.select('transaction_date').distinct().collect():
                part = data.where(F.col('transaction_date') == F.lit(row[0])).drop('transaction_date')
                frames[name+'/transaction_date='+str(row[0])] = spark.createDataFrame(part.collect(), part.schema)
        else:
            frames[name] = spark.createDataFrame(data.collect(), data.schema)
        directories.add(name)
        files.add(name+'/_SUCCESS')
    def read(reader, *paths, **kwargs):
        result = frames[str(Path(paths[0]))]
        for path in paths[1:]:
            result = result.unionByName(frames[str(Path(path))])
        return result
    def write_json(fs, path, obj):
        import copy
        documents[str(path)] = copy.deepcopy(obj)
        files.add(str(path))
    monkeypatch.setattr(module, '_store_filesystem', lambda session, path: (fs, Path(path)))
    monkeypatch.setattr(module, '_read_json', lambda session, fs, path: documents[str(path)])
    monkeypatch.setattr(module, '_write_json', write_json)
    monkeypatch.setattr(DataFrameWriter, 'partitionBy', partition)
    monkeypatch.setattr(DataFrameWriter, 'parquet', write)
    monkeypatch.setattr(DataFrameReader, 'parquet', read)


def test_store_real_roundtrip_retry_overlap_and_snapshot(spark, tmp_path, storage):
    first, batch, store = [str(tmp_path / p) for p in ('first','batch','store')]
    seed(spark).write.parquet(first)
    initial = ingest_store(spark, first, store, 'seed', '2011-12-11', True, 2)
    extra = frame(spark, [('A','I1',datetime(2011,12,10,9),1,0.004,'P3'),
                          ('A','I3',datetime(2011,12,11,9),1,0.004,'P1'),
                          ('C','I4',datetime(2011,12,11,9),1,20.0,'P1')])
    seed(spark).unionByName(extra).write.parquet(batch)
    updated = ingest_store(spark, batch, store, 'day11', '2011-12-12', False, 2)
    assert updated['version'] != initial['version']
    assert ingest_store(spark, batch, store, 'day11','2011-12-12',False,2)['version'] == updated['version']
    # An entirely duplicated batch with a different ID still cannot add money.
    ingest_store(spark, batch, store, 'overlap','2011-12-12',False,2)
    actual, _ = read_store_rfm(spark,store,'2011-12-12')
    expected = compute_rfm(seed(spark).unionByName(extra),'2011-12-12')
    assert actual.exceptAll(expected).isEmpty() and expected.exceptAll(actual).isEmpty()
    old, _ = read_store_rfm(spark,store,'2011-12-11')
    assert old.count() == 2 and old.where("CustomerID='A'").first().Frequency == 1
    assert set(r.CustomerID for r in read_store_active(spark,updated,'2011-12-11','2011-12-12').collect()) == {'A','C'}
    assert read_store_active(spark,updated,'2011-12-13','2011-12-14').isEmpty()
    # Untouched date partitions are referenced rather than rewritten.
    for key, old_bucket in initial['buckets'].items():
        if '2011-12-09' in old_bucket['transactions']:
            # This example replayed seed rows too, so the date is touched;
            # correctness is checked above even with overlapping inputs.
            assert '2011-12-09' in updated['buckets'][key]['transactions']
    with pytest.raises(ValueError, match='Conflicting'):
        ingest_store(spark, first, store,'day11','2011-12-12',False,2)
    with pytest.raises(ValueError, match='backwards'):
        ingest_store(spark, first, store,'back','2011-12-11',False,2)


def test_store_only_updates_new_date_and_guards(spark,tmp_path,storage):
    source, batch, future, store = [str(tmp_path/p) for p in ('source','new','future','store')]
    seed(spark).write.parquet(source)
    delta = frame(spark,[('A','I3',datetime(2011,12,11,9),1,1.0,None)])
    delta.unionByName(delta).write.parquet(batch)
    frame(spark,[('A','I4',datetime(2011,12,12),1,1.0,'P')]).write.parquet(future)
    with pytest.raises(ValueError,match='First ingestion'):
        ingest_store(spark,source,store,'seed','2011-12-11',False,1)
    initial = ingest_store(spark,source,store,'seed','2011-12-11',True,1)
    updated = ingest_store(spark,batch,store,'new','2011-12-12',False,1)
    assert initial['buckets']['0']['transactions']['2011-12-10'] == updated['buckets']['0']['transactions']['2011-12-10']
    # Null optional fields must compare null-safely across different batch IDs.
    ingest_store(spark,batch,store,'new_replay','2011-12-12',False,1)
    rfm, _ = read_store_rfm(spark,store,'2011-12-12')
    assert rfm.where("CustomerID='A'").first().Frequency == 2
    assert rfm.where("CustomerID='A'").first().Monetary == Decimal('1.00')
    for kwargs, pattern in [({'input_path':future},'on/after'),
                            ({'num_buckets':2},'Bucket count'),
                            ({'initialize':True},'already initialized')]:
        opts = dict(input_path=batch,store_path=store,batch_id='bad',run_date='2011-12-12',initialize=False,num_buckets=1)
        opts.update(kwargs)
        with pytest.raises(ValueError,match=pattern):
            ingest_store(spark,**opts)
    import scripts.pyspark_rfm as module
    fs, root = module._store_filesystem(spark,store)
    lock = spark._jvm.org.apache.hadoop.fs.Path(root,'_writer.lock')
    assert fs.createNewFile(lock)
    try:
        with pytest.raises(ValueError,match='locked'):
            ingest_store(spark,batch,store,'locked','2011-12-12',False,1)
        assert fs.exists(lock)  # a second writer cannot remove somebody else's lock
    finally:
        fs.delete(lock,False)


def test_store_failed_write_not_published(spark,tmp_path,storage):
    source, store = str(tmp_path/'source'),str(tmp_path/'store')
    seed(spark).write.parquet(source)
    with patch('pyspark.sql.readwriter.DataFrameWriter.parquet',side_effect=RuntimeError('write failed')):
        with pytest.raises(RuntimeError,match='write failed'):
            ingest_store(spark,source,store,'seed','2011-12-11',True,1)
    with pytest.raises(ValueError,match='No committed'):
        load_manifest(spark,store)
    # Owned lock was released; same seed can be retried.
    initial = ingest_store(spark,source,store,'seed','2011-12-11',True,1)
    assert initial['version'] == '000000000001'
    with patch('pyspark.sql.readwriter.DataFrameWriter.parquet',side_effect=RuntimeError('write failed')):
        with pytest.raises(RuntimeError,match='write failed'):
            ingest_store(spark,source,store,'failed_update','2011-12-12',False,1)
    assert load_manifest(spark,store)['version'] == initial['version']


def test_store_cli_rejects_removed_modes_and_invalid_batch():
    base = ['--store-path', 'store', '--output', 'out', '--model-path', 'model',
            '--model-version', 'v1', '--run-date', '2011-12-12']
    for extra in [['--history-path', 'old'], ['--initialize-history'],
                  ['--input', 'new', '--batch-id', '../bad']]:
        with pytest.raises(SystemExit):
            parse_args(["--input", "new", "--batch-id", "b1"] + base + extra)
    assert parse_args(["--input", "new", "--batch-id", "b1"] + [x for x in base if x not in ('--store-path', 'store')]).store_path


@pytest.mark.parametrize('path', ['models/v1', 'models/v1/', r'C:\models\v1', 'hdfs://host/models/v1'])
def test_cli_model_version_from_directory(path):
    args = parse_args(["--input", "new", "--batch-id", "b1"] + ['--run-date','2011-12-12','--model-path',path])
    assert args.model_version == 'v1'

def test_cli_defaults_and_explicit_overrides():
    from scripts.pyspark_rfm import DATA_ROOT
    args = parse_args(["--input", "new", "--batch-id", "b1"] + ['--run-date','2011-12-12','--model-path','models/v1'])
    assert args.store_path == str(DATA_ROOT / 'curated' / 'rfm_store')
    assert args.output == str(DATA_ROOT / 'rfm')
    assert args.num_buckets is None
    assert args.period_start == '2011-12-11'
    explicit = parse_args(["--input", "new", "--batch-id", "b1"] + ['--run-date','2011-12-12','--model-path','models/v1',
                          '--model-version','release1','--output','out','--store-path','existing'])
    assert (explicit.model_version,explicit.output,explicit.store_path)==('release1','out','existing')
    monthly = training_args(['--run-date','2012-01-01','--k','4','--model-output','models/v2'])
    assert monthly.store_path == args.store_path and monthly.num_buckets is None

@pytest.mark.parametrize('day', ['2011-02-30','20111212'])
def test_cli_invalid_cutoff_with_explicit_period(day):
    with pytest.raises(SystemExit):
        parse_args(["--input", "new", "--batch-id", "b1"] + ['--run-date',day,'--period-start','2011-12-01','--model-path','models/v1'])

def test_store_infers_existing_bucket_count(spark,tmp_path,storage):
    first,store=[str(tmp_path / p) for p in ('seed','store')]
    seed(spark).write.parquet(first)
    initialized=ingest_store(spark,first,store,'seed','2011-12-11',True,2)
    updated=ingest_store(spark,first,store,'next','2011-12-12',False,None)
    assert initialized['num_buckets']==updated['num_buckets']==2
    assert read_store_table(spark,updated,'state').count()==2


def test_daily_cli_requires_input_and_batch_but_training_can_read_store():
    base = ['--run-date','2011-12-12','--model-path','models/v1']
    for extra in [[], ['--input','new'], ['--batch-id','b1']]:
        with pytest.raises(SystemExit):
            parse_args(base + extra)
    daily = parse_args(base + ['--input','new','--batch-id','b1'])
    assert (daily.input,daily.batch_id)==('new','b1')
    monthly = training_args(['--run-date','2012-01-01','--k','4','--model-output','models/v2'])
    assert monthly.input is None and monthly.batch_id is None
