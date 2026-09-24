"""Tests for curated input, RFM metrics, relative scores, and CLI arguments."""
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

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


# add_rfm_segment bây giờ chỉ phân cụm (K-means), chỉ trả về cột Cluster
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
def test_add_rfm_segment_requires_explicit_k(spark):
    df = rfm_rows(spark, [(str(i), 10, i + 1, float(i + 1)) for i in range(4)])
    with pytest.raises(TypeError):
        add_rfm_segment(df)


def test_add_rfm_segment_rejects_k_below_two(spark):
    df = rfm_rows(spark, [(str(i), 10, i + 1, float(i + 1)) for i in range(4)])
    with pytest.raises(ValueError, match="at least 2"):
        add_rfm_segment(df, k=1)


def test_add_rfm_segment_rejects_k_larger_than_customer_count(spark):
    df = rfm_rows(spark, [("a", 10, 1, 10.0), ("b", 5, 2, 20.0)])
    with pytest.raises(ValueError, match="exceeds the number of customers"):
        add_rfm_segment(df, k=3)


@pytest.mark.parametrize("distinct_vectors", [1, 2])
def test_add_rfm_segment_rejects_insufficient_distinct_features(spark, distinct_vectors):
    df = rfm_rows(spark, [
        (str(i), 10, 1 + i % distinct_vectors, 50.0)
        for i in range(6)
    ])
    with pytest.raises(ValueError, match="distinct feature vectors"):
        add_rfm_segment(df, k=3)


def test_add_rfm_segment_rejects_fewer_occupied_clusters(spark, monkeypatch):
    from pyspark.ml import Pipeline

    class CollapsedModel:
        def transform(self, prepared):
            return prepared.withColumn("Cluster", F.lit(0))

    monkeypatch.setattr(Pipeline, "fit", lambda self, dataset: CollapsedModel())
    df = rfm_rows(spark, [("a", 1, 20, 5000.0), ("b", 300, 1, 5.0)])
    with pytest.raises(ValueError, match="1 occupied clusters; expected 2"):
        add_rfm_segment(df, k=2)


# Output chỉ có thêm đúng 1 cột mới: Cluster. Không có ClusterRank, không
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
    result_df = add_rfm_segment(df, k=2)

    assert set(result_df.columns) - set(df.columns) == {"Cluster"}
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
    output = add_rfm_segment(output, k=2)
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
    )


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
    output = add_rfm_segment(output, k=2)
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
    first_output = add_rfm_segment(first_output, k=2)
    second_output = add_rfm_segment(second_output, k=2)

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
        ["--input", "in", "--output", "out", "--run-date", "2011-12-10", "--k", "4"]
    )
    assert args.run_date == "2011-12-10"
    assert args.k == 4
    with pytest.raises(SystemExit):
        parse_args(
            ["--input", "in", "--output", "out", "--run-date", "2011-02-30", "--k", "4"]
        )


# --k không có giá trị mặc định: thiếu cờ này phải bị CLI từ chối ngay,
# và --k < 2 cũng phải bị từ chối (K-means cần ít nhất 2 cụm).
def test_cli_k_is_required_and_validated():
    with pytest.raises(SystemExit):
        parse_args(["--input", "in", "--output", "out", "--run-date", "2011-12-10"])
    with pytest.raises(SystemExit):
        parse_args(
            ["--input", "in", "--output", "out", "--run-date", "2011-12-10", "--k", "1"]
        )
