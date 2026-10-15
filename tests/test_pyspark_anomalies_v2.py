"""Test cho scripts/pyspark_anomalies.py: phát hiện bất thường (anomaly) trên giao dịch retail.

Chiến lược (vì build_output chạy chậm ~30-60s do localCheckpoint):
  * Dựng MỘT bộ dữ liệu "golden" có chủ đích, mỗi nhóm dòng được thiết kế để kích hoạt (hoặc
    KHÔNG kích hoạt) đúng một quy tắc, rồi chạy build_output đúng 1 lần và kiểm tra nhiều thứ.
  * Giá trị mong đợi được TÍNH TAY từ định nghĩa (median, MAD, z-score, giá trị rủi ro...),
    không chép từ kết quả code chạy ra.
  * Hàm nhỏ (_normalized, _check, _deviation, robust_baseline...) test riêng bằng dữ liệu tí hon.

Bố cục:
  0. Helper và dữ liệu golden
  1. CLI và hợp đồng đầu vào: parse_args, validate_paths, validate_input_contract, select_history
  2. Hàm nhỏ: _finite, _normalized, _flags, _first, _severity_from_value, _check, _deviation, robust_baseline
  3. Kết quả golden: cấu trúc, phân loại record_type/channel, ngữ cảnh mô tả
  4. Quy tắc Tier 1 (data) và Tier 2 (business)
  5. Quy tắc thống kê: quantity_deviation / price_deviation (+ tham số z, min_samples)
  6. assess_orders: so sánh tổng hóa đơn
  7. validate_output, write_results, _log_results
  8. main() và nhánh --pipeline-context
"""
import logging
import math
import runpy
import statistics
import sys
import types
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import pyspark_anomalies as an

RUN_DATE = "2011-12-10"
MIN_SAMPLES = 5  # nhỏ hơn mặc định (30) để dữ liệu test gọn; các hằng số MIN_CUSTOMER_* vẫn là giá trị thật.
OUTPUT_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice",
    "CustomerID", "Country", "run_date", "reference_mode", "z_threshold", "min_samples",
    "line_value", "record_type", "channel", "has_dotcom_postage", "data_quality_flags",
    "context_flags", "check_results", "data_anomaly_flags", "business_anomaly_flags",
    "anomaly_flags", "max_severity", "assessment_status",
]
ORDER_COLUMNS = [
    "invoice_key", "channel", "customer_key", "line_count", "invalid_lines", "customer_ids",
    "countries", "invoice_days", "eligible", "order_total", "reason", "context_level",
    "reference_count", "reference_median", "reference_mad", "robust_z", "fold_change",
    "value_at_risk", "severity", "currency", "z_threshold", "run_date", "reference_mode", "status",
]


# ---------------------------------------------------------------------------
# 0. HELPER VÀ DỮ LIỆU GOLDEN
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-pyspark-anomalies-v2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module", autouse=True)
def ansi_mode(spark):  # Chạy cả module với ANSI SQL bật (đúng như main() cấu hình khi chạy thật), rồi trả về giá trị cũ để không ảnh hưởng file test khác.
    previous = spark.conf.get("spark.sql.ansi.enabled", None)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    yield
    if previous is None:
        spark.conf.unset("spark.sql.ansi.enabled")
    else:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def utc(*parts):  # Tạo datetime UTC ngắn gọn.
    return datetime(*parts, tzinfo=timezone.utc)


def tx(invoice, stock, description, quantity, when, price, customer, country="United Kingdom"):  # Tạo 1 dòng giao dịch đúng thứ tự cột của TRANSACTION_SCHEMA.
    return (invoice, stock, description, quantity, when, price, customer, country)


def frame(spark, rows):  # Tạo DataFrame giao dịch với schema chuẩn của pipeline.
    return spark.createDataFrame(rows, an.TRANSACTION_SCHEMA)


def scalar(spark, column):  # Tính giá trị của một biểu thức Column trên 1 dòng (để test hàm biểu thức nhỏ).
    return spark.range(1).select(column.alias("v")).first().v


def pick(rows, invoice, stock=None, quantity=None):  # Tìm đúng 1 dòng kết quả theo (InvoiceNo, StockCode, Quantity); báo lỗi nếu 0 hoặc nhiều dòng khớp.
    matches = [r for r in rows if r.InvoiceNo == invoice
               and (stock is None or r.StockCode == stock)
               and (quantity is None or r.Quantity == quantity)]
    assert len(matches) == 1, f"{invoice}/{stock}/{quantity}: {len(matches)} matches"
    return matches[0]


def chk(row, rule):  # Lấy kết quả của một rule trong check_results của dòng.
    return next(c for c in row.check_results if c.rule == rule)


def golden_rows():  # Bộ dữ liệu golden: mỗi khối (A..N) kích hoạt đúng một nhóm quy tắc; xem chú thích từng khối.
    rows, add = [], None
    add = rows.append
    add(tx("10000", "E1", "EARLY ITEM", 1, utc(2011, 1, 3), 1.0, "CE"))  # dòng sớm nhất: mốc đầu lịch sử cho cửa sổ trả hàng
    # Khối A: (C100, S1) 5 dòng SL=2 + 1 dòng SL=20; MAD = 0 -> cờ theo "lớn hơn median"
    for i in range(5):
        add(tx(str(10001 + i), "S1", "MUG", 2, utc(2011, 12, 1 + i), 2.0, "C100"))
    add(tx("10006", "S1", "MUG", 20, utc(2011, 12, 6), 2.0, "C100"))
    # Khối B: (C101, S2) SL 10,10,11,11,12,12 + outlier 100; MAD > 0 -> cờ theo z-score; value = 267 (medium)
    for i, q in enumerate([10, 10, 11, 11, 12, 12]):
        add(tx(str(10011 + i), "S2", "PLATE", q, utc(2011, 12, 1 + i), 3.0, "C101"))
    add(tx("10017", "S2", "PLATE", 100, utc(2011, 12, 7), 3.0, "C101"))
    # Khối C: (C102, S3) như B nhưng outlier 500 -> value = 1467 (high)
    for i, q in enumerate([10, 10, 11, 11, 12, 12]):
        add(tx(str(10021 + i), "S3", "BOWL", q, utc(2011, 12, 1 + i), 3.0, "C102"))
    add(tx("10027", "S3", "BOWL", 500, utc(2011, 12, 7), 3.0, "C102"))
    # Khối D: (C103, S4) giá 2.0 x5, rồi 4.0 (cao), 2.2 (lệch nhỏ < 30%), 1.0 (thấp); SL luôn 1
    for i in range(5):
        add(tx(str(10031 + i), "S4", "CUP", 1, utc(2011, 12, 1 + i), 2.0, "C103"))
    add(tx("10036", "S4", "CUP", 1, utc(2011, 12, 6), 4.0, "C103"))
    add(tx("10037", "S4", "CUP", 1, utc(2011, 12, 7), 2.2, "C103"))
    add(tx("10038", "S4", "CUP", 1, utc(2011, 12, 8), 1.0, "C103"))
    # Khối E: khách vãng lai (retail) mã R1: SL 3 x5 + SL 30 -> baseline cấp channel_stock
    for i in range(5):
        add(tx(str(10041 + i), "R1", "RETAIL ITEM", 3, utc(2011, 12, 1 + i), 1.5, None))
    add(tx("10046", "R1", "RETAIL ITEM", 30, utc(2011, 12, 6), 1.5, None))
    # Khối F: chỉ 1 dòng của (C200, N1) -> không đủ lịch sử
    add(tx("10051", "N1", "NEW ITEM", 1, utc(2011, 12, 2), 5.0, "C200"))
    # Khối G: quy tắc dữ liệu (Tier 1)
    add(tx("10061", None, "NO STOCK", 1, utc(2011, 12, 8), 2.0, "C300"))
    add(tx("10062", "G2", "NO COUNTRY", 1, utc(2011, 12, 8), 2.0, "C300", None))
    add(tx("10063", "G3", "NO DATE ITEM", 1, None, 2.0, "C300"))
    add(tx("10064", "G4", "NO QTY", None, utc(2011, 12, 8), 2.0, "C300"))
    add(tx("10065", "G5", "NAN PRICE", 1, utc(2011, 12, 8), float("nan"), "C300"))
    add(tx("10066", "G5B", "INF PRICE", 1, utc(2011, 12, 8), float("inf"), "C300"))
    add(tx("10067", "G6", None, 1, utc(2011, 12, 8), 2.0, "C300"))
    add(tx("C10068", "G7", "CANCEL POSITIVE", 2, utc(2011, 12, 8), 2.0, "C300"))
    add(tx("10069", "G7B", "NEGATIVE NORMAL", -2, utc(2011, 12, 8), 3.0, "C300"))
    add(tx("10070", "G8", "NEG PRICE", 1, utc(2011, 12, 8), -5.0, "C300"))
    add(tx("A10071", "B", "ADJUST BAD DEBT", 1, utc(2011, 12, 8), -100.0, None))
    add(tx("10072", "B", "MANUAL B", 1, utc(2011, 12, 8), 10.0, None))
    add(tx("10073", "G8C", "ADJUST BAD DEBT", 1, utc(2011, 12, 8), 7.0, None))
    add(tx("10080", "G9A", "ITEM A", 1, utc(2011, 12, 8), 2.0, "C301"))
    add(tx("10080", "G9B", "ITEM B", 1, utc(2011, 12, 8), 2.0, "C302"))
    add(tx("10081", "G9C", "ITEM C", 1, utc(2011, 12, 8), 2.0, "C303"))
    add(tx("10081", "G9D", "ITEM D", 1, utc(2011, 12, 8), 2.0, None))
    add(tx("10082", "G9E", "ITEM E", 1, utc(2011, 12, 8), 2.0, "C304"))
    add(tx(" 10082 ", "G9F", "ITEM F", 1, utc(2011, 12, 8), 2.0, "C305"))
    add(tx("10090", "G10A", "ITEM", 1, utc(2011, 12, 8), 2.0, "C306", "United Kingdom"))
    add(tx("10090", "G10B", "ITEM", 1, utc(2011, 12, 8), 2.0, "C306", "France"))
    add(tx("10091", "G10C", "ITEM", 1, utc(2011, 12, 3), 2.0, "C306"))
    add(tx("10091", "G10D", "ITEM", 1, utc(2011, 12, 4), 2.0, "C306"))
    # Khối H: loại dòng, kênh, và quy tắc vận hành (Tier 2)
    add(tx("10100", "POST", "POSTAGE", 1, utc(2011, 12, 8), 18.0, "C400"))
    add(tx("10101", "X1", "PACKING CHARGE", 1, utc(2011, 12, 8), 3.0, "C400"))
    add(tx("10102", "AMAZONFEE", "AMAZON FEE", 1, utc(2011, 12, 8), 50.0, "C400"))
    add(tx("10110", "DOT", "DOTCOM POSTAGE", 1, utc(2011, 12, 8), 20.0, None))
    add(tx("10110", "W1", "WEB ITEM", 2, utc(2011, 12, 8), 5.0, None))
    add(tx("10120", "S1", "CHECKED", -5, utc(2011, 12, 8), 0.0, None))       # kiểm kho, giá ước tính 10 (low)
    add(tx("10121", "ZZ9", "COUNTED", -3, utc(2011, 12, 8), 0.0, None))      # kiểm kho, không biết giá
    add(tx("10122", "S2", "ADJUSTED", -400, utc(2011, 12, 8), 0.0, None))    # 1200 (high)
    add(tx("10123", "S2", "ADJUSTED", -50, utc(2011, 12, 8), 0.0, None))     # 150 (medium)
    add(tx("10130", "S1", "MUG", 1, utc(2011, 12, 8), 2.0, "C401"))
    add(tx("10130", "S1", "MUG", 5, utc(2011, 12, 8), 0.0, "C401"))          # dòng giá 0 trong hóa đơn có dòng trả tiền
    add(tx("10131", "S1", "MUG", 3, utc(2011, 12, 8), 0.0, "C401"))          # hóa đơn toàn giá 0
    add(tx("10140", "RT1", "RETURN ITEM", 3, utc(2011, 12, 2), 4.0, "C500"))
    add(tx("C10141", "RT1", "RETURN ITEM", -10, utc(2011, 12, 5), 4.0, "C500"))   # trả 10 > mua 3
    add(tx("10142", "RT2", "RETURN ITEM TWO", 10, utc(2011, 12, 2), 4.0, "C501"))
    add(tx("C10143", "RT2", "RETURN ITEM TWO", -4, utc(2011, 12, 5), 4.0, "C501"))  # trả 4 <= mua 10
    add(tx("C10144", "RT1", "RETURN ITEM", -1, utc(2011, 12, 5), 4.0, None))
    add(tx("C10145", "POST", "POSTAGE", -1, utc(2011, 12, 5), 18.0, "C500"))
    add(tx("C10146", "RT3", "RETURN ITEM", -1, utc(2011, 1, 10), 2.0, "C502"))      # trong 30 ngày đầu lịch sử
    add(tx("C10147", "RT4", "RETURN ITEM", -1, None, 4.0, "C500"))
    # Khối J: từ khóa trong mô tả
    for i in range(3):
        add(tx(str(10150 + i), "K1", "STOCK POT", 1, utc(2011, 12, 8), 2.0, "C600"))
    add(tx("10153", "K1", "DAMAGED", -3, utc(2011, 12, 8), 0.0, None))
    add(tx("10154", "K2", "CHECKED", -1, utc(2011, 12, 8), 0.0, None))
    # Khối M, N: hóa đơn trống và số lượng 0
    add(tx(None, "U1", "UNKNOWN INVOICE", 1, utc(2011, 12, 8), 2.0, "C700"))
    add(tx("10160", "UC1", "ZERO QTY", 0, utc(2011, 12, 8), 2.0, "C800"))
    # Khối cutoff: chỉ dòng trước 2011-12-10 00:00 được giữ
    add(tx("10997", "CUT1", "AFTER", 1, utc(2011, 12, 15), 2.0, "C900"))
    add(tx("10999", "CUT2", "ON CUTOFF", 1, utc(2011, 12, 10), 2.0, "C900"))
    add(tx("10998", "CUT3", "JUST BEFORE", 1, utc(2011, 12, 9, 23, 59, 59), 2.0, "C900"))
    return rows


def mini_rows():  # Bộ nhỏ (khối B + E) để chạy thêm các biến thể tham số z_threshold/min_samples.
    rows = [r for r in golden_rows() if r[0] in {str(10011 + i) for i in range(7)}
            | {str(10041 + i) for i in range(6)}]
    rows.append(tx("10000", "E1", "EARLY ITEM", 1, utc(2011, 1, 3), 1.0, "CE"))
    return rows


def expected_history(rows):  # Các dòng đầu vào thuộc lịch sử: ngày null hoặc trước ngày chốt (đúng định nghĩa select_history).
    cutoff = utc(2011, 12, 10)
    return [r for r in rows if r[4] is None or r[4] < cutoff]


@pytest.fixture(scope="module")
def golden(spark):  # Chạy build_output MỘT lần trên bộ golden, cache và collect để mọi test chỉ việc đọc.
    rows = golden_rows()
    source = frame(spark, rows)
    result = an.build_output(source, RUN_DATE, an.Z_THRESHOLD, MIN_SAMPLES).cache()
    collected = result.collect()
    return SimpleNamespace(raw=rows, source=source, result=result, rows=collected)


# ---------------------------------------------------------------------------
# 1. CLI VÀ HỢP ĐỒNG ĐẦU VÀO
# ---------------------------------------------------------------------------

BASE_ARGS = ["--input", "in", "--output", "out", "--run-date", RUN_DATE]


def test_parse_args_defaults():  # Tham số bắt buộc hợp lệ cho giá trị mặc định: z-threshold 3.5, min-samples 30.
    args = an.parse_args(BASE_ARGS)

    assert (args.input, args.output, args.run_date) == ("in", "out", RUN_DATE)
    assert args.z_threshold == an.Z_THRESHOLD == 3.5
    assert args.min_samples == 30


def test_parse_args_accepts_overrides():  # --z-threshold và --min-samples tường minh ghi đè mặc định.
    args = an.parse_args(BASE_ARGS + ["--z-threshold", "5", "--min-samples", "2"])

    assert (args.z_threshold, args.min_samples) == (5.0, 2)


@pytest.mark.parametrize("missing", ["--input", "--output", "--run-date"])
def test_parse_args_requires_each_mandatory_option(missing):  # Thiếu bất kỳ tham số bắt buộc nào (input, output, run-date) thì argparse thoát mã 2.
    argv = list(BASE_ARGS)
    index = argv.index(missing)
    del argv[index:index + 2]

    with pytest.raises(SystemExit) as excinfo:
        an.parse_args(argv)

    assert excinfo.value.code == 2


@pytest.mark.parametrize("day", ["2011-02-30", "20111210", "2011-1-1", "", "abc"])
def test_parse_args_rejects_invalid_run_date(day):  # run-date phải là ngày ISO chuẩn YYYY-MM-DD có thật.
    with pytest.raises(SystemExit):
        an.parse_args(["--input", "in", "--output", "out", "--run-date", day])


@pytest.mark.parametrize("z", ["0", "-1", "nan", "inf", "-inf"])
def test_parse_args_rejects_nonpositive_or_nonfinite_z(z):  # z-threshold phải dương và hữu hạn.
    with pytest.raises(SystemExit):
        an.parse_args(BASE_ARGS + ["--z-threshold", z])


@pytest.mark.parametrize("n", ["1", "0", "-5"])
def test_parse_args_rejects_min_samples_below_two(n):  # min-samples phải >= 2 (cần ít nhất 2 mẫu mới tính được median/MAD).
    with pytest.raises(SystemExit):
        an.parse_args(BASE_ARGS + ["--min-samples", n])


def test_validate_paths_accepts_separate_local_paths(tmp_path):  # Hai thư mục tách biệt (kể cả anh em có tên bắt đầu giống nhau) hợp lệ.
    an.validate_paths(str(tmp_path / "input"), str(tmp_path / "output"))
    an.validate_paths(str(tmp_path / "store"), str(tmp_path / "store2"))


@pytest.mark.parametrize("make_target", [
    lambda base: base,
    lambda base: base / "child",
    lambda base: base.parent,
], ids=["identical", "output_inside_input", "output_is_parent_of_input"])
def test_validate_paths_rejects_overlap(tmp_path, make_target):  # Input và output trùng hoặc lồng nhau (cả hai chiều) bị từ chối để partition không ghi đè dữ liệu nguồn.
    base = tmp_path / "data"

    with pytest.raises(ValueError, match="overlap"):
        an.validate_paths(str(base), str(make_target(base)))


@pytest.mark.parametrize("remote", ["s3://bucket/x", "hdfs://host/x", "gs://b/x"])
def test_validate_paths_rejects_non_local_paths(tmp_path, remote):  # Ghi đè partition an toàn chỉ hỗ trợ đường dẫn local; s3/hdfs/gs bị từ chối ở cả input lẫn output.
    with pytest.raises(ValueError, match="Only local"):
        an.validate_paths(remote, str(tmp_path / "out"))
    with pytest.raises(ValueError, match="Only local"):
        an.validate_paths(str(tmp_path / "in"), remote)


def test_validate_input_contract_accepts_canonical_and_extra_columns(spark):  # Schema chuẩn (kể cả có cột thừa) được chấp nhận.
    df = frame(spark, [tx("1", "A", "X", 1, utc(2011, 12, 1), 1.0, "C")])

    an.validate_input_contract(df)
    an.validate_input_contract(df.withColumn("extra", F.lit(1)))


def test_validate_input_contract_rejects_missing_columns(spark):  # Thiếu cột bắt buộc thì báo lỗi nêu tên cột thiếu.
    df = frame(spark, [tx("1", "A", "X", 1, utc(2011, 12, 1), 1.0, "C")]).drop("Country")

    with pytest.raises(ValueError, match="Missing transaction columns.*Country"):
        an.validate_input_contract(df)


@pytest.mark.parametrize("column,expr,actual,expected", [
    ("Quantity", "CAST(Quantity AS BIGINT)", "bigint", "int"),
    ("UnitPrice", "CAST(UnitPrice AS FLOAT)", "float", "double"),
    ("InvoiceDate", "CAST(InvoiceDate AS STRING)", "string", "timestamp"),
    ("CustomerID", "CAST(CustomerID AS BINARY)", "binary", "string"),
])
def test_validate_input_contract_rejects_wrong_types(spark, column, expr, actual, expected):  # Sai kiểu dữ liệu ở bất kỳ cột nào báo lỗi nêu rõ kiểu thực tế và kiểu mong đợi.
    df = frame(spark, [tx("1", "A", "X", 1, utc(2011, 12, 1), 1.0, "C")]).withColumn(column, F.expr(expr))

    with pytest.raises(ValueError, match=f"{column} has type {actual}; expected {expected}"):
        an.validate_input_contract(df)


def test_select_history_keeps_null_dates_and_excludes_cutoff_and_later(spark):  # select_history giữ dòng ngày null và dòng trước ngày chốt (kể cả 23:59:59), loại dòng đúng 00:00:00 ngày chốt và sau đó.
    df = frame(spark, [
        tx("1", "A", "X", 1, utc(2011, 12, 9, 23, 59, 59), 1.0, "C"),
        tx("2", "A", "X", 1, utc(2011, 12, 10), 1.0, "C"),
        tx("3", "A", "X", 1, utc(2011, 12, 11), 1.0, "C"),
        tx("4", "A", "X", 1, None, 1.0, "C"),
    ])

    kept = {r.InvoiceNo for r in an.select_history(df, RUN_DATE).collect()}

    assert kept == {"1", "4"}


# ---------------------------------------------------------------------------
# 2. HÀM NHỎ
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [
    (1.0, True), (0.0, True), (-3.5, True),
    (float("nan"), False), (float("inf"), False), (float("-inf"), False), (None, False),
])
def test_finite_accepts_only_finite_numbers(spark, value, expected):  # _finite đúng với số hữu hạn và sai với NaN, vô cực, null (null cho False chứ không phải null).
    assert scalar(spark, an._finite(F.lit(value).cast("double"))) is expected


@pytest.mark.parametrize("raw,collapse,expected", [
    ("  ab  c ", False, "AB  C"),
    ("  ab  c ", True, "AB C"),
    ("\u00a0ab\u00a0", False, "AB"),
    ("   ", False, None),
    ("", True, None),
    (None, False, None),
])
def test_normalized_trims_uppercases_and_nulls_blanks(spark, raw, collapse, expected):  # _normalized cắt khoảng trắng đầu/cuối (kể cả NBSP Unicode), viết hoa, tùy chọn gom khoảng trắng giữa chữ, và đổi chuỗi trống thành null.
    assert scalar(spark, an._normalized(F.lit(raw).cast("string"), collapse)) == expected


def test_flags_keeps_only_true_conditions_in_order(spark):  # _flags trả mảng tên các điều kiện đúng theo thứ tự khai báo; điều kiện false hoặc null bị bỏ.
    result = scalar(spark, an._flags((
        ("a", F.lit(True)), ("b", F.lit(False)),
        ("c", F.lit(None).cast("boolean")), ("d", F.lit(True)),
    )))

    assert result == ["a", "d"]


def test_first_returns_first_matching_choice(spark):  # _first chọn giá trị của điều kiện ĐÚNG đầu tiên; không điều kiện nào đúng thì null.
    choices = [(F.lit(False), F.lit("a")), (F.lit(True), F.lit("b")), (F.lit(True), F.lit("c"))]

    assert scalar(spark, an._first(choices)) == "b"
    assert scalar(spark, an._first([(F.lit(False), F.lit("a"))])) is None


@pytest.mark.parametrize("value,expected", [
    (1000.0, "high"), (5000.0, "high"), (999.99, "medium"), (100.0, "medium"),
    (99.99, "low"), (0.0, "low"), (-50.0, "low"), (None, "low"),
])
def test_severity_from_value_thresholds(spark, value, expected):  # Ngưỡng mức độ: >= 1000 high, >= 100 medium, còn lại (kể cả âm và null) là low; kiểm tra đúng biên.
    assert scalar(spark, an._severity_from_value(F.lit(value).cast("double"))) == expected


def check_of(spark, reason=None, flagged=True, **kwargs):  # Dựng một kết quả _check từ literal và trả về struct.
    reason_col = F.lit(reason).cast("string")
    return spark.range(1).select(
        an._check("business", "demo", reason_col, F.lit(flagged), **kwargs).alias("c")).first().c


def test_check_flagged_uses_value_at_risk_for_severity(spark):  # Rule bị cờ: status "flagged", mức độ lấy từ value_at_risk, các bằng chứng (median, fold...) được giữ.
    result = check_of(spark, observed=F.lit(12), context_level=F.lit("customer_stock"),
                      reference_count=F.lit(7), reference_median=F.lit(2.0),
                      fold_change=F.lit(6.0), value_at_risk=F.lit(150.0))

    assert result.status == "flagged" and result.severity == "medium"
    assert (result.tier, result.rule) == ("business", "demo")
    assert result.observed_value == 12.0 and result.reference_count == 7
    assert result.reference_median == 2.0 and result.fold_change == 6.0
    assert result.context_level == "customer_stock" and result.value_at_risk == 150.0


def test_check_not_applied_hides_evidence_and_severity(spark):  # Có lý do không áp dụng thì status "not_applied", mọi bằng chứng (value_at_risk, level, count...) và severity bị null dù flagged=True; observed vẫn giữ.
    result = check_of(spark, reason="insufficient_history", observed=F.lit(5),
                      context_level=F.lit("x"), reference_count=F.lit(3), value_at_risk=F.lit(5000.0))

    assert result.status == "not_applied" and result.reason == "insufficient_history"
    assert result.severity is None and result.value_at_risk is None
    assert result.context_level is None and result.reference_count is None
    assert result.observed_value == 5.0


def test_check_not_flagged_has_no_severity(spark):  # Áp dụng nhưng không bị cờ thì status "not_flagged" và không có severity.
    result = check_of(spark, flagged=False, value_at_risk=F.lit(5000.0))

    assert result.status == "not_flagged" and result.severity is None


def test_check_null_flag_is_treated_as_not_flagged(spark):  # Điều kiện cờ null (không xác định) được coi là KHÔNG bị cờ.
    result = spark.range(1).select(an._check(
        "data", "demo", F.lit(None).cast("string"), F.lit(None).cast("boolean")).alias("c")).first().c

    assert result.status == "not_flagged"


def test_check_explicit_severity_overrides_value_based_severity(spark):  # severity truyền tường minh (ví dụ "high") thắng mức suy ra từ giá trị rủi ro nhỏ.
    result = check_of(spark, value_at_risk=F.lit(1.0), severity=F.lit("high"))

    assert result.severity == "high"


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_check_nonfinite_numbers_become_null(spark, bad):  # Số NaN/vô cực trong các trường số (observed, fold, value_at_risk...) được đổi thành null thay vì lan ra output.
    result = check_of(spark, observed=F.lit(bad), fold_change=F.lit(bad), value_at_risk=F.lit(bad))

    assert result.observed_value is None and result.fold_change is None
    assert result.value_at_risk is None


def deviation(spark, x, median, mad, two_sided, z=3.5):  # Chạy _deviation với literal và trả (z, vượt ngưỡng, fold).
    cols = an._deviation(F.lit(x), F.lit(median), F.lit(mad), z, two_sided=two_sided)
    row = spark.range(1).select(*[c.alias(f"c{i}") for i, c in enumerate(cols)]).first()
    return row.c0, row.c1, row.c2


def test_deviation_robust_z_and_fold_change(spark):  # Khi MAD > 0: z = 0.6745*(x-median)/MAD, vượt ngưỡng khi z > 3.5; fold = exp(x-median) làm tròn 6 số.
    z, exceeds, fold = deviation(spark, 5.0, 2.0, 1.0, two_sided=False)
    assert z == pytest.approx(0.6745 * 3.0) and exceeds is False
    assert fold == pytest.approx(math.exp(3.0), abs=1e-6)

    z, exceeds, _ = deviation(spark, 10.0, 2.0, 1.0, two_sided=False)
    assert z == pytest.approx(0.6745 * 8.0) and exceeds is True


def test_deviation_one_sided_ignores_low_values_two_sided_flags_them(spark):  # Một phía chỉ bắt giá trị cao; hai phía bắt cả thấp (dùng |z|).
    assert deviation(spark, -6.0, 2.0, 1.0, two_sided=False)[1] is False
    assert deviation(spark, -6.0, 2.0, 1.0, two_sided=True)[1] is True


@pytest.mark.parametrize("x,one_sided,two_sided", [
    (3.0, True, True), (1.0, False, True), (2.0, False, False),
])
def test_deviation_with_zero_mad_compares_to_median(spark, x, one_sided, two_sided):  # MAD = 0 thì z là null và so sánh trực tiếp: một phía chỉ bắt x > median, hai phía bắt x != median; x = median thì không bao giờ bị cờ.
    z1, exceeds_one, _ = deviation(spark, x, 2.0, 0.0, two_sided=False)
    _, exceeds_two, _ = deviation(spark, x, 2.0, 0.0, two_sided=True)

    assert z1 is None
    assert exceeds_one is one_sided and exceeds_two is two_sided


def test_robust_baseline_computes_exact_median_and_mad_per_group(spark):  # robust_baseline tính median/MAD CHÍNH XÁC theo nhóm, bỏ qua giá trị null/NaN và khóa null; nhóm 1 phần tử có MAD = 0.
    df = spark.createDataFrame([
        ("a", 1.0), ("a", 2.0), ("a", 3.0), ("a", 4.0), ("a", 100.0),
        ("a", float("nan")), ("a", None), (None, 7.0), ("b", 5.0),
    ], "k string, v double")

    rows = {r.k: r for r in an.robust_baseline(df, ["k"], F.col("v"), "__t").collect()}

    assert set(rows) == {"a", "b"}
    assert (rows["a"].__getitem__("__t_n"), rows["a"].__getitem__("__t_med"),
            rows["a"].__getitem__("__t_mad")) == (5, 3.0, 1.0)
    assert (rows["b"].__getitem__("__t_n"), rows["b"].__getitem__("__t_mad")) == (1, 0.0)


# ---------------------------------------------------------------------------
# 3. KẾT QUẢ GOLDEN: CẤU TRÚC, record_type, channel, ngữ cảnh mô tả
# ---------------------------------------------------------------------------


def test_output_has_expected_columns_and_metadata(golden):  # Output có đúng danh sách và thứ tự cột, kèm metadata chạy (run_date, reference_mode, z_threshold, min_samples).
    assert golden.result.columns == OUTPUT_COLUMNS
    assert {(r.run_date, r.reference_mode, r.z_threshold, r.min_samples) for r in golden.rows} == {
        (date(2011, 12, 10), "retrospective_before_run_date", 3.5, MIN_SAMPLES)}


def test_output_preserves_every_history_row_and_drops_future_rows(golden):  # Giữ nguyên MỌI dòng lịch sử (cả dòng ngày null, kể cả bản trùng khóa), không thêm bớt, và loại các dòng từ ngày chốt trở đi.
    assert len(golden.rows) == len(expected_history(golden.raw))
    invoices = {r.InvoiceNo for r in golden.rows}
    assert "10998" in invoices                       # 23:59:59 trước ngày chốt được giữ
    assert "10999" not in invoices and "10997" not in invoices
    assert pick(golden.rows, "10063").InvoiceDate is None  # dòng ngày null vẫn được đánh giá


def test_every_row_has_all_rules_in_fixed_order(golden):  # Mỗi dòng có đủ 14 kết quả rule theo đúng thứ tự RULES (7 data + 7 business) với tier tương ứng.
    assert len(an.RULES) == 14
    for row in golden.rows:
        assert [c.rule for c in row.check_results] == list(an.RULES)
        assert [c.tier for c in row.check_results] == ["data"] * 7 + ["business"] * 7


def test_summary_columns_are_consistent_with_check_results(golden):  # Các cột tổng hợp khớp check_results: anomaly_flags = data + business; status "flagged" khi và chỉ khi có cờ; severity chỉ có ở rule bị cờ; max_severity là mức cao nhất.
    rank = {"low": 1, "medium": 2, "high": 3}
    for row in golden.rows:
        flagged = [c for c in row.check_results if c.status == "flagged"]
        assert row.anomaly_flags == row.data_anomaly_flags + row.business_anomaly_flags
        assert sorted(row.anomaly_flags) == sorted(c.rule for c in flagged)
        assert (row.assessment_status == "flagged") == bool(flagged)
        assert all((c.severity is not None) == (c.status == "flagged") for c in row.check_results)
        expected_max = max((c.severity for c in flagged), key=rank.get, default=None)
        assert row.max_severity == expected_max
        assert row.assessment_status in {"flagged", "not_flagged", "not_assessable"}


def test_golden_output_passes_validate_output(golden):  # Output golden thỏa mọi kiểm tra của validate_output (bảo toàn dòng, thứ tự rule, status/severity hợp lệ).
    an.validate_output(golden.source, golden.result, RUN_DATE)


@pytest.mark.parametrize("invoice,stock,quantity,record_type", [
    ("10001", "S1", None, "sale"),
    ("10100", "POST", None, "fee_service"),
    ("10101", "X1", None, "fee_service"),
    ("10102", "AMAZONFEE", None, "fee_service"),
    ("10110", "DOT", None, "fee_service"),
    ("10120", "S1", None, "inventory_adjustment"),
    ("10130", "S1", 1, "sale"),
    ("10130", "S1", 5, "zero_price_line"),
    ("10131", "S1", None, "zero_price_line"),
    ("C10141", "RT1", None, "cancellation"),
    ("A10071", "B", None, "accounting_adjustment"),
    ("10072", "B", None, "accounting_adjustment"),
    ("10073", "G8C", None, "accounting_adjustment"),
    ("10160", "UC1", None, "unclassified"),
    ("10069", "G7B", None, "unclassified"),
    ("10064", "G4", None, "unclassified"),
    ("10065", "G5", None, "unclassified"),
])
def test_record_type_classification(golden, invoice, stock, quantity, record_type):  # record_type theo thứ tự ưu tiên: kế toán > hủy > phí/dịch vụ > kiểm kho > bán > dòng giá 0 > chưa phân loại (kể cả mã dịch vụ, mô tả ADJUST BAD DEBT, số lượng 0, giá NaN).
    assert pick(golden.rows, invoice, stock, quantity).record_type == record_type


@pytest.mark.parametrize("invoice,stock,channel", [
    ("10001", "S1", "identified"),
    ("10041", "R1", "retail_other"),
    ("10110", "DOT", "retail_web"),
    ("10110", "W1", "retail_web"),
    ("10120", "S1", "internal"),
    ("A10071", "B", "internal"),
    ("10072", "B", "internal"),
    (None, "U1", "unknown"),
    ("10080", "G9A", "identified"),
    ("C10144", "RT1", "retail_other"),
])
def test_channel_assignment(golden, invoice, stock, channel):  # Kênh theo hóa đơn: identified (có CustomerID) > retail_web (có DOT) > retail_other (có dòng giá dương, không kế toán) > internal; hóa đơn rỗng là unknown.
    assert pick(golden.rows, invoice, stock).channel == channel


def test_dotcom_postage_flag_applies_to_whole_invoice(golden):  # has_dotcom_postage đúng cho CẢ hóa đơn có dòng DOT (cả dòng hàng thường), và sai cho hóa đơn khác.
    assert pick(golden.rows, "10110", "DOT").has_dotcom_postage is True
    assert pick(golden.rows, "10110", "W1").has_dotcom_postage is True
    assert pick(golden.rows, "10001", "S1").has_dotcom_postage is False
    assert pick(golden.rows, None, "U1").has_dotcom_postage is False


@pytest.mark.parametrize("invoice,stock,quantity,flags", [
    ("10061", None, None, {"missing_stock_code"}),
    ("10062", "G2", None, {"missing_country"}),
    ("10063", "G3", None, {"missing_invoice_date"}),
    ("10064", "G4", None, {"invalid_quantity", "invalid_line_value"}),
    ("10065", "G5", None, {"invalid_unit_price", "invalid_line_value"}),
    ("10066", "G5B", None, {"invalid_unit_price", "invalid_line_value"}),
    ("10067", "G6", None, {"missing_description"}),
    (None, "U1", None, {"missing_invoice_no"}),
    ("10001", "S1", None, set()),
])
def test_data_quality_flags_describe_each_defect(golden, invoice, stock, quantity, flags):  # data_quality_flags liệt kê đúng từng khiếm khuyết của dòng (thiếu mã/quốc gia/ngày/mô tả/hóa đơn, số lượng hoặc giá không hợp lệ); dòng sạch thì rỗng.
    assert set(pick(golden.rows, invoice, stock, quantity).data_quality_flags) == flags


@pytest.mark.parametrize("invoice,stock,quantity,expected,absent", [
    ("C10141", "RT1", None, {"cancelled_invoice"}, set()),
    ("10100", "POST", None, {"service_line"}, {"special_stock_code"}),
    ("10102", "AMAZONFEE", None, {"special_stock_code"}, {"service_line"}),
    ("10120", "S1", None, {"zero_negative", "stock_words"}, {"keyword_unresolved"}),
    ("10130", "S1", 5, {"zero_positive"}, {"sale_shape"}),
    ("10001", "S1", None, {"sale_shape"}, set()),
    ("10150", "K1", None, {"keyword_product_name"}, {"stock_words"}),
    ("10153", "K1", None, {"damage_words", "zero_negative"}, {"keyword_product_name"}),
    ("10154", "K2", None, {"keyword_unresolved"}, {"stock_words"}),
    ("10121", "ZZ9", None, {"keyword_unresolved"}, {"stock_words"}),
])
def test_context_flags_explain_row_context(golden, invoice, stock, quantity, expected, absent):  # context_flags giải thích ngữ cảnh: hủy, dịch vụ, mã đặc biệt, giá 0 âm/dương, dạng bán hàng, và từ khóa mô tả (tên sản phẩm hợp lệ chứa từ khóa KHÔNG bị cờ; từ khóa lạ có tham chiếu thì cờ theo nhóm; chưa có tham chiếu thì "unresolved").
    flags = set(pick(golden.rows, invoice, stock, quantity).context_flags)

    assert expected <= flags
    assert not (absent & flags)


# ---------------------------------------------------------------------------
# 4. QUY TẮC TIER 1 (DATA) VÀ TIER 2 (BUSINESS)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("invoice,stock", [
    ("10061", None), ("10062", "G2"), ("10063", "G3"), ("10064", "G4"), (None, "U1"),
])
def test_missing_required_field_flagged_high(golden, invoice, stock):  # Thiếu mã hàng, quốc gia, ngày, số lượng hoặc số hóa đơn đều bị cờ missing_required_field mức high.
    row = pick(golden.rows, invoice, stock)

    assert chk(row, "missing_required_field").status == "flagged"
    assert chk(row, "missing_required_field").severity == "high"
    assert row.max_severity == "high"


def test_missing_required_field_not_flagged_for_complete_row(golden):  # Dòng đầy đủ không bị cờ missing_required_field (status not_flagged).
    assert chk(pick(golden.rows, "10001", "S1"), "missing_required_field").status == "not_flagged"


def test_missing_description_flagged_low(golden):  # Thiếu mô tả chỉ là cờ low; dòng này không có cờ nào cao hơn nên max_severity cũng là low.
    row = pick(golden.rows, "10067", "G6")

    assert chk(row, "missing_description").status == "flagged"
    assert chk(row, "missing_description").severity == "low"
    assert row.max_severity == "low"
    assert chk(pick(golden.rows, "10001", "S1"), "missing_description").status == "not_flagged"


@pytest.mark.parametrize("invoice,stock", [("10064", "G4"), ("10065", "G5"), ("10066", "G5B")])
def test_invalid_numeric_value_flagged_for_null_quantity_nan_and_inf(golden, invoice, stock):  # Số lượng null, giá NaN hoặc vô cực bị cờ invalid_numeric_value mức high.
    check = chk(pick(golden.rows, invoice, stock), "invalid_numeric_value")

    assert check.status == "flagged" and check.severity == "high"


def test_invalid_numeric_makes_dependent_rules_not_applied(golden):  # Khi số liệu không hợp lệ, các rule phụ thuộc (dấu hủy, giá âm) chuyển thành not_applied với lý do invalid_numeric_value thay vì đoán.
    assert chk(pick(golden.rows, "10064", "G4"), "cancel_sign_conflict").reason == "invalid_numeric_value"
    assert chk(pick(golden.rows, "10065", "G5"), "negative_price_outside_adjustment").status == "not_applied"
    assert chk(pick(golden.rows, "10065", "G5"), "negative_price_outside_adjustment").reason == "invalid_numeric_value"


@pytest.mark.parametrize("invoice,stock", [("C10068", "G7"), ("10069", "G7B")])
def test_cancel_sign_conflict_flagged_medium(golden, invoice, stock):  # Hóa đơn hủy (C...) có số lượng >= 0, hoặc hóa đơn thường có số lượng âm (không phải kiểm kho/kế toán), bị cờ cancel_sign_conflict mức medium.
    check = chk(pick(golden.rows, invoice, stock), "cancel_sign_conflict")

    assert check.status == "flagged" and check.severity == "medium"
    assert check.observed_value == pick(golden.rows, invoice, stock).Quantity


def test_cancel_sign_conflict_not_flagged_for_legitimate_negatives(golden):  # Số lượng âm hợp lệ: hóa đơn hủy với SL âm, và dòng kiểm kho (giá 0, không khách) không bị cờ.
    assert chk(pick(golden.rows, "C10141", "RT1"), "cancel_sign_conflict").status == "not_flagged"
    assert chk(pick(golden.rows, "10120", "S1"), "cancel_sign_conflict").status == "not_flagged"


def test_negative_price_flagged_high_except_accounting(golden):  # Giá âm ngoài bút toán kế toán bị cờ high; dòng kế toán (hóa đơn A...) có giá âm thì không bị cờ rule này.
    flagged = chk(pick(golden.rows, "10070", "G8"), "negative_price_outside_adjustment")
    accounting = chk(pick(golden.rows, "A10071", "B"), "negative_price_outside_adjustment")

    assert flagged.status == "flagged" and flagged.severity == "high" and flagged.observed_value == -5.0
    assert accounting.status == "not_flagged"


def test_invoice_multiple_customers_flagged_for_every_line(golden):  # Một hóa đơn có >= 2 CustomerID khác nhau thì MỌI dòng của hóa đơn đó bị cờ; observed là số khách, reference_count là số dòng.
    for stock in ("G9A", "G9B"):
        check = chk(pick(golden.rows, "10080", stock), "invoice_multiple_customers")
        assert check.status == "flagged" and check.severity == "medium"
        assert check.observed_value == 2.0 and check.reference_count == 2


def test_invoice_multiple_customers_normalizes_invoice_number(golden):  # Số hóa đơn "10082" và " 10082 " (có khoảng trắng) được gộp thành một hóa đơn khi so sánh khách hàng.
    assert chk(pick(golden.rows, "10082", "G9E"), "invoice_multiple_customers").status == "flagged"
    assert chk(pick(golden.rows, " 10082 ", "G9F"), "invoice_multiple_customers").status == "flagged"


def test_invoice_multiple_customers_not_applied_when_ids_incomplete(golden):  # Hóa đơn chỉ một số dòng có CustomerID (không đủ thông tin để kết luận) thì not_applied với lý do incomplete_customer_ids.
    for stock in ("G9C", "G9D"):
        check = chk(pick(golden.rows, "10081", stock), "invoice_multiple_customers")
        assert check.status == "not_applied" and check.reason == "incomplete_customer_ids"


@pytest.mark.parametrize("invoice,stock", [("10090", "G10A"), ("10090", "G10B"),
                                           ("10091", "G10C"), ("10091", "G10D")])
def test_invoice_inconsistent_header_flagged_for_country_or_date_mismatch(golden, invoice, stock):  # Cùng hóa đơn nhưng khác quốc gia hoặc khác ngày thì mọi dòng bị cờ invoice_inconsistent_header mức medium.
    check = chk(pick(golden.rows, invoice, stock), "invoice_inconsistent_header")

    assert check.status == "flagged" and check.severity == "medium" and check.reference_count == 2


def test_rules_needing_invoice_are_not_applied_without_invoice_number(golden):  # Dòng không có số hóa đơn thì các rule dựa trên hóa đơn chuyển sang not_applied với lý do missing_invoice_no.
    row = pick(golden.rows, None, "U1")

    for rule in ("cancel_sign_conflict", "invoice_multiple_customers", "invoice_inconsistent_header"):
        assert chk(row, rule).status == "not_applied" and chk(row, rule).reason == "missing_invoice_no"
    for rule in ("quantity_deviation", "price_deviation"):
        assert chk(row, rule).reason == "missing_invoice_no"


@pytest.mark.parametrize("invoice,stock,quantity,value,severity", [
    ("10120", "S1", None, 10.0, "low"),       # 5 x giá bán trung vị 2.0
    ("10122", "S2", None, 1200.0, "high"),    # 400 x 3.0
    ("10123", "S2", None, 150.0, "medium"),   # 50 x 3.0
])
def test_inventory_adjustment_estimates_value_from_median_sale_price(golden, invoice, stock, quantity, value, severity):  # Dòng kiểm kho luôn bị cờ; giá trị ước tính = |SL| x giá bán trung vị của mã đó và mức độ suy ra từ giá trị (10 low, 150 medium, 1200 high).
    check = chk(pick(golden.rows, invoice, stock, quantity), "inventory_adjustment")

    assert check.status == "flagged"
    assert check.value_at_risk == pytest.approx(value) and check.severity == severity


def test_inventory_adjustment_unknown_price_has_null_value_and_low_severity(golden):  # Mã chưa từng bán (không biết giá) vẫn bị cờ nhưng value_at_risk là null và severity rơi về "low" (hành vi hiện tại của code).
    check = chk(pick(golden.rows, "10121", "ZZ9"), "inventory_adjustment")

    assert check.status == "flagged" and check.value_at_risk is None and check.severity == "low"


@pytest.mark.parametrize("invoice,stock,value", [
    ("A10071", "B", 100.0), ("10072", "B", 10.0), ("10073", "G8C", 7.0),
])
def test_manual_accounting_entry_always_high(golden, invoice, stock, value):  # Bút toán kế toán thủ công (hóa đơn A..., mã B, mô tả ADJUST BAD DEBT) luôn bị cờ mức high bất kể giá trị; value_at_risk là |line_value|.
    check = chk(pick(golden.rows, invoice, stock), "manual_accounting_entry")

    assert check.status == "flagged" and check.severity == "high"
    assert check.value_at_risk == pytest.approx(value)


def test_zero_price_sale_line_vs_all_zero_invoice(golden):  # Dòng giá 0 trong hóa đơn CÓ dòng trả tiền bị cờ zero_price_sale_line; hóa đơn TOÀN giá 0 bị cờ all_zero_price_invoice; hai rule loại trừ nhau; giá trị = |SL| x giá bán trung vị.
    mixed = pick(golden.rows, "10130", "S1", 5)
    all_zero = pick(golden.rows, "10131", "S1")

    assert chk(mixed, "zero_price_sale_line").status == "flagged"
    assert chk(mixed, "zero_price_sale_line").value_at_risk == pytest.approx(10.0)
    assert chk(mixed, "all_zero_price_invoice").status == "not_flagged"
    assert chk(all_zero, "all_zero_price_invoice").status == "flagged"
    assert chk(all_zero, "all_zero_price_invoice").value_at_risk == pytest.approx(6.0)
    assert chk(all_zero, "all_zero_price_invoice").reference_count == 1
    assert chk(all_zero, "zero_price_sale_line").status == "not_flagged"


def test_zero_price_rules_outside_scope_for_normal_sales(golden):  # Dòng bán bình thường không thuộc phạm vi các rule giá 0/kiểm kho/kế toán/trả hàng: not_applied với lý do outside_scope.
    row = pick(golden.rows, "10001", "S1")

    for rule in ("zero_price_sale_line", "all_zero_price_invoice", "inventory_adjustment",
                 "manual_accounting_entry", "unmatched_return"):
        assert chk(row, rule).status == "not_applied" and chk(row, rule).reason == "outside_scope"


def test_unmatched_return_flags_returns_exceeding_purchases(golden):  # Trả nhiều hơn đã mua (10 > 3) bị cờ: observed = tổng trả của cặp khách x mã, reference_count = tổng đã mua, value = |line_value|; trả ít hơn đã mua (4 <= 10) thì không bị cờ.
    flagged = chk(pick(golden.rows, "C10141", "RT1"), "unmatched_return")
    matched = chk(pick(golden.rows, "C10143", "RT2"), "unmatched_return")

    assert flagged.status == "flagged" and flagged.severity == "low"
    assert (flagged.observed_value, flagged.reference_count) == (10.0, 3)
    assert flagged.value_at_risk == pytest.approx(40.0)
    assert matched.status == "not_flagged" and (matched.observed_value, matched.reference_count) == (4.0, 10)


@pytest.mark.parametrize("invoice,stock,reason", [
    ("C10144", "RT1", "no_customer_history"),
    ("C10145", "POST", "service_or_special_code"),
    ("C10147", "RT4", "missing_invoice_date"),
    ("C10146", "RT3", "history_window_start"),
])
def test_unmatched_return_not_applied_reasons(golden, invoice, stock, reason):  # Trả hàng không đánh giá được khi: không có khách, là mã dịch vụ, thiếu ngày, hoặc nằm trong 30 ngày đầu của lịch sử (chưa đủ lịch sử mua để so).
    check = chk(pick(golden.rows, invoice, stock), "unmatched_return")

    assert check.status == "not_applied" and check.reason == reason


# ---------------------------------------------------------------------------
# 5. QUY TẮC THỐNG KÊ: quantity_deviation / price_deviation
# ---------------------------------------------------------------------------


def test_quantity_deviation_with_zero_mad_flags_high_quantity_only(golden):  # Khối A (MAD = 0): chỉ dòng SL=20 bị cờ (z null, fold 10, value 36 low, cấp customer_stock); 5 dòng SL=2 còn lại không bị cờ.
    flagged = chk(pick(golden.rows, "10006", "S1"), "quantity_deviation")

    assert flagged.status == "flagged" and flagged.severity == "low"
    assert flagged.context_level == "customer_stock" and flagged.reference_count == 6
    assert flagged.robust_z is None and flagged.reference_mad == 0.0
    assert flagged.reference_median == pytest.approx(2.0)
    assert flagged.fold_change == pytest.approx(10.0) and flagged.value_at_risk == pytest.approx(36.0)
    for i in range(5):
        assert chk(pick(golden.rows, str(10001 + i), "S1"), "quantity_deviation").status == "not_flagged"


def robust_stats(values):  # Đáp án độc lập: median, MAD (trên log) tính bằng thư viện statistics.
    logs = [math.log(v) for v in values]
    median = statistics.median(logs)
    return median, statistics.median([abs(x - median) for x in logs])


@pytest.mark.parametrize("invoice,stock,outlier,quantities,price,value,severity", [
    ("10017", "S2", 100, [10, 10, 11, 11, 12, 12], 3.0, 267.0, "medium"),
    ("10027", "S3", 500, [10, 10, 11, 11, 12, 12], 3.0, 1467.0, "high"),
])
def test_quantity_deviation_with_positive_mad_uses_robust_z(golden, invoice, stock, outlier, quantities, price, value, severity):  # Khối B/C (MAD > 0): z-score = 0.6745*(x-median)/MAD khớp đáp án tính độc lập; value = (SL - median) x giá; mức độ medium/high theo giá trị.
    median, mad = robust_stats(quantities + [outlier])
    check = chk(pick(golden.rows, invoice, stock), "quantity_deviation")

    assert check.status == "flagged" and check.severity == severity
    assert check.reference_count == 7 and check.context_level == "customer_stock"
    assert check.reference_mad == pytest.approx(mad)
    assert check.robust_z == pytest.approx(0.6745 * (math.log(outlier) - median) / mad)
    assert check.fold_change == pytest.approx(outlier / math.exp(median), abs=1e-6)
    assert check.value_at_risk == pytest.approx((outlier - math.exp(median)) * price, abs=0.01)
    assert pick(golden.rows, invoice, stock).max_severity == severity


def test_quantity_deviation_normal_lines_not_flagged(golden):  # Các dòng SL bình thường (kể cả 12 > median 11 và SL thấp hơn) không bị cờ vì z nhỏ hơn ngưỡng và quy tắc là một phía.
    for i in range(6):
        row = pick(golden.rows, str(10011 + i), "S2")
        assert chk(row, "quantity_deviation").status == "not_flagged"


def test_quantity_deviation_falls_back_to_channel_stock_for_retail(golden):  # Khách vãng lai (retail) không có cấp khách hàng nên dùng baseline channel_stock: SL 30 so với median 3 bị cờ (fold 10, value 40.5).
    check = chk(pick(golden.rows, "10046", "R1"), "quantity_deviation")

    assert check.status == "flagged" and check.context_level == "channel_stock"
    assert check.reference_count == 6 and check.fold_change == pytest.approx(10.0)
    assert check.reference_median == pytest.approx(3.0)
    assert check.value_at_risk == pytest.approx((30 - 3) * 1.5)
    assert pick(golden.rows, "10046", "R1").channel == "retail_other"


def test_price_deviation_flags_large_deviation_in_both_directions(golden):  # Giá cao gấp đôi (4.0 so với 2.0) và giá thấp một nửa (1.0) đều bị cờ price_deviation (hai phía); value = |giá - median| x SL.
    high = chk(pick(golden.rows, "10036", "S4"), "price_deviation")
    low = chk(pick(golden.rows, "10038", "S4"), "price_deviation")

    assert high.status == "flagged" and high.fold_change == pytest.approx(2.0)
    assert high.value_at_risk == pytest.approx(2.0) and high.severity == "low"
    assert high.context_level == "customer_stock" and high.reference_count == 8
    assert low.status == "flagged" and low.fold_change == pytest.approx(0.5)
    assert low.value_at_risk == pytest.approx(1.0)


def test_price_deviation_requires_materiality(golden):  # Giá lệch nhỏ (2.2 so với 2.0, chỉ 10% < ngưỡng 30%) KHÔNG bị cờ dù khác median (ngưỡng trọng yếu cố định trước).
    check = chk(pick(golden.rows, "10037", "S4"), "price_deviation")

    assert check.status == "not_flagged" and check.fold_change == pytest.approx(1.1)
    assert an.PRICE_MIN_RELATIVE == 0.30


def test_deviation_not_applied_for_thin_history_and_special_cases(golden):  # Không đủ lịch sử, ngày null hoặc dòng không phải "sale" thì rule thống kê chuyển not_applied với lý do tương ứng.
    assert chk(pick(golden.rows, "10051", "N1"), "quantity_deviation").reason == "insufficient_history"
    assert chk(pick(golden.rows, "10051", "N1"), "price_deviation").reason == "insufficient_history"
    assert chk(pick(golden.rows, "10061", None), "quantity_deviation").reason == "insufficient_history"
    assert chk(pick(golden.rows, "10063", "G3"), "quantity_deviation").reason == "missing_invoice_date"
    assert chk(pick(golden.rows, "10100", "POST"), "quantity_deviation").reason == "outside_scope"
    assert pick(golden.rows, "10051", "N1").assessment_status == "not_flagged"


def test_materiality_constants_are_fixed():  # Các ngưỡng trọng yếu và thống kê được cố định (đổi giá trị phải là quyết định có chủ đích, test này sẽ báo).
    assert (an.Z_THRESHOLD, an.MAD_CONSTANT) == (3.5, 0.6745)
    assert (an.QUANTITY_MIN_FOLD, an.PRICE_MIN_RELATIVE, an.ORDER_MIN_FOLD) == (3.0, 0.30, 3.0)
    assert (an.MIN_CUSTOMER_STOCK_LINES, an.MIN_CUSTOMER_LINES, an.MIN_CUSTOMER_INVOICES) == (5, 20, 5)
    assert (an.SEVERITY_HIGH_GBP, an.SEVERITY_MEDIUM_GBP, an.RETURN_WINDOW_DAYS) == (1000.0, 100.0, 30)


@pytest.fixture(scope="module")
def mini_variants(spark):  # Chạy build_output trên bộ nhỏ với 3 cấu hình (mặc định, z rất lớn, min_samples lớn) và trả về từng kết quả đã collect.
    source = frame(spark, mini_rows())
    runs = {}
    for name, z, samples in (("base", 3.5, 5), ("huge_z", 100.0, 5), ("many_samples", 3.5, 30)):
        runs[name] = an.build_output(source, RUN_DATE, z, samples).collect()
    return runs


def test_variant_default_flags_both_levels(mini_variants):  # Cấu hình mặc định: dòng 10017 (customer_stock, z > 3.5) và 10046 (channel_stock) đều bị cờ.
    rows = mini_variants["base"]

    assert chk(pick(rows, "10017"), "quantity_deviation").status == "flagged"
    assert chk(pick(rows, "10046"), "quantity_deviation").status == "flagged"


def test_variant_huge_z_only_affects_positive_mad_groups(mini_variants):  # z-threshold = 100 làm dòng có MAD > 0 (10017, z ~ 17) hết bị cờ, nhưng nhóm MAD = 0 (10046) vẫn bị cờ vì khi đó không dùng z-score.
    rows = mini_variants["huge_z"]

    assert chk(pick(rows, "10017"), "quantity_deviation").status == "not_flagged"
    assert chk(pick(rows, "10046"), "quantity_deviation").status == "flagged"
    assert {r.z_threshold for r in rows} == {100.0}


def test_variant_min_samples_only_affects_channel_level(mini_variants):  # min-samples = 30 làm baseline channel_stock (6 mẫu) không đủ nên not_applied insufficient_history; baseline customer_stock vẫn dùng ngưỡng riêng (5 dòng) nên 10017 vẫn bị cờ.
    rows = mini_variants["many_samples"]

    assert chk(pick(rows, "10046"), "quantity_deviation").reason == "insufficient_history"
    assert chk(pick(rows, "10017"), "quantity_deviation").status == "flagged"
    assert {r.min_samples for r in rows} == {30}


# ---------------------------------------------------------------------------
# 6. assess_orders: SO SÁNH TỔNG HÓA ĐƠN
# ---------------------------------------------------------------------------


def orders_rows():  # Dữ liệu hóa đơn: K1 (5 đơn 100 + 1 đơn 2000), K2 (2 đơn), K7 (MAD > 0 + outlier 5000), và các ca không đánh giá được.
    rows = []
    for i in range(5):
        rows.append(tx(str(20001 + i), "P", "ITEM", 10, utc(2011, 12, 1 + i), 10.0, "K1"))
    rows.append(tx("20006", "P", "ITEM", 20, utc(2011, 12, 6), 100.0, "K1"))
    rows += [tx("20011", "P", "ITEM", 10, utc(2011, 12, 1), 10.0, "K2"),
             tx("20012", "P", "ITEM", 10, utc(2011, 12, 2), 10.0, "K2")]
    for i, price in enumerate([10.0, 11.0, 12.0, 10.0, 11.0, 12.0]):
        rows.append(tx(str(20041 + i), "P", "ITEM", 10, utc(2011, 12, 1 + i), price, "K7"))
    rows.append(tx("20047", "P", "ITEM", 10, utc(2011, 12, 7), 500.0, "K7"))
    rows += [
        tx("C20021", "P", "ITEM", -1, utc(2011, 12, 3), 5.0, "K3"),
        tx("20022", "P", "ITEM", 0, utc(2011, 12, 3), 5.0, "K3"),
        tx("20023", "P", "ITEM", 1, utc(2011, 12, 3), 5.0, "K4"),
        tx("20023", "Q", "ITEM", 1, utc(2011, 12, 3), 5.0, "K5"),
        tx("20024", "P", "ITEM", 1, utc(2011, 12, 3), 5.0, "K4", "United Kingdom"),
        tx("20024", "Q", "ITEM", 1, utc(2011, 12, 3), 5.0, "K4", "France"),
        tx("20025", "P", "ITEM", 1, utc(2011, 12, 3), 5.0, "K4"),
        tx("20025", "Q", "ITEM", 1, utc(2011, 12, 4), 5.0, "K4"),
        tx("A20026", "P", "ITEM", 1, utc(2011, 12, 3), 5.0, None),
        tx("20027", "P", "ITEM", 1, utc(2011, 12, 3), 5.0, None),
        tx("20028", "P", "ITEM", 1, utc(2011, 12, 15), 5.0, "K6"),
        tx("c20029", "P", "ITEM", -1, utc(2011, 12, 3), 5.0, "K3"),
        tx(None, "P", "ITEM", 1, utc(2011, 12, 3), 5.0, "K3"),
        tx("   ", "P", "ITEM", 1, utc(2011, 12, 3), 5.0, "K3"),
    ]
    return rows


@pytest.fixture(scope="module")
def orders(spark):  # Chạy assess_orders một lần trên dữ liệu hóa đơn và trả về dict invoice_key -> dòng.
    result = an.assess_orders(frame(spark, orders_rows()), RUN_DATE, 3.5, 5)
    return SimpleNamespace(df=result, by_key={r.invoice_key: r for r in result.collect()})


def test_assess_orders_columns_and_metadata(orders):  # Output assess_orders có đúng cột và metadata cố định (GBP, z_threshold, run_date, reference_mode).
    assert orders.df.columns == ORDER_COLUMNS
    for row in orders.by_key.values():
        assert (row.currency, row.z_threshold, row.run_date, row.reference_mode) == (
            "GBP", 3.5, date(2011, 12, 10), "retrospective_before_run_date")


def test_assess_orders_one_row_per_invoice_and_drops_blank_keys(orders):  # Mỗi hóa đơn một dòng, khóa hóa đơn được chuẩn hóa (trim + viết hoa), và dòng có số hóa đơn rỗng/null bị loại.
    assert set(orders.by_key) == (
        {str(20001 + i) for i in range(6)} | {"20011", "20012"} | {str(20041 + i) for i in range(7)}
        | {"C20021", "20022", "20023", "20024", "20025", "A20026", "20027", "20028", "C20029"})
    assert orders.df.count() == len(orders.by_key)


def test_assess_orders_flags_customer_outlier_with_zero_mad(orders):  # K1: 5 đơn 100 và 1 đơn 2000 (MAD = 0): đơn 2000 bị cờ (fold 20, value 1900, high, z null); 5 đơn còn lại không bị cờ; cấp tham chiếu là "customer" với 6 đơn.
    flagged = orders.by_key["20006"]

    assert flagged.status == "flagged" and flagged.severity == "high"
    assert (flagged.context_level, flagged.reference_count) == ("customer", 6)
    assert flagged.order_total == pytest.approx(2000.0) and flagged.reference_median == pytest.approx(100.0)
    assert flagged.fold_change == pytest.approx(20.0) and flagged.value_at_risk == pytest.approx(1900.0)
    assert flagged.robust_z is None and flagged.reference_mad == 0.0
    for i in range(5):
        normal = orders.by_key[str(20001 + i)]
        assert normal.status == "not_flagged" and normal.severity is None
        assert normal.order_total == pytest.approx(100.0) and normal.fold_change == pytest.approx(1.0)


def test_assess_orders_positive_mad_uses_robust_z(orders):  # K7 (MAD > 0): z-score, fold và value_at_risk của đơn 5000 khớp đáp án tính độc lập từ median/MAD của log tổng đơn.
    totals = [100.0, 110.0, 120.0, 100.0, 110.0, 120.0, 5000.0]
    median, mad = robust_stats(totals)
    row = orders.by_key["20047"]

    assert row.status == "flagged" and row.severity == "high" and row.context_level == "customer"
    assert row.reference_mad == pytest.approx(mad)
    assert row.robust_z == pytest.approx(0.6745 * (math.log(5000.0) - median) / mad)
    assert row.fold_change == pytest.approx(5000.0 / math.exp(median), abs=1e-6)
    assert row.value_at_risk == pytest.approx(5000.0 - math.exp(median), abs=0.01)
    assert orders.by_key["20041"].status == "not_flagged"


def test_assess_orders_falls_back_to_channel_for_new_customer(orders):  # K2 chỉ có 2 đơn (< 5) nên so với baseline kênh "identified" (15 đơn đủ điều kiện): context_level = "channel", không bị cờ.
    row = orders.by_key["20011"]

    assert row.status == "not_flagged" and row.context_level == "channel"
    assert row.reference_count == 15 and row.channel == "identified"


@pytest.mark.parametrize("invoice,reason", [
    ("C20021", "cancelled_invoice"),
    ("C20029", "cancelled_invoice"),          # "c20029" viết thường được chuẩn hóa
    ("20022", "invalid_invoice_lines"),
    ("20028", "invalid_invoice_lines"),       # dòng từ ngày chốt trở đi bị coi là không hợp lệ
    ("20023", "multiple_customer_ids"),
    ("20024", "inconsistent_country"),
    ("20025", "inconsistent_invoice_date"),
    ("A20026", "outside_scope"),
    ("20027", "insufficient_history"),
])
def test_assess_orders_not_assessable_reasons(orders, invoice, reason):  # Hóa đơn không đánh giá được theo thứ tự lý do: hủy, dòng không hợp lệ, nhiều khách, khác quốc gia, khác ngày, ngoài phạm vi (kế toán), thiếu lịch sử; các trường bằng chứng đều null.
    row = orders.by_key[invoice]

    assert row.status == "not_assessable" and row.reason == reason
    assert row.severity is None and row.context_level is None and row.value_at_risk is None
    assert row.fold_change is None and row.reference_median is None


def test_assess_orders_eligibility_and_channels(orders):  # eligible/channel/order_total: hóa đơn hủy hoặc lỗi không đủ điều kiện (order_total null); kế toán là internal; khách vãng lai không DOT là retail_other.
    assert orders.by_key["C20021"].eligible is False and orders.by_key["C20021"].order_total is None
    assert orders.by_key["20028"].eligible is False
    assert orders.by_key["A20026"].channel == "internal" and orders.by_key["A20026"].eligible is True
    assert orders.by_key["20027"].channel == "retail_other"
    assert orders.by_key["20023"].customer_ids == 2 and orders.by_key["20024"].countries == 2


# ---------------------------------------------------------------------------
# 7. validate_output, write_results, _log_results
# ---------------------------------------------------------------------------


def test_validate_output_rejects_lost_row(golden):  # Mất một dòng so với đầu vào thì bị từ chối vì không bảo toàn tập dòng.
    with pytest.raises(ValueError, match="preserve the input row multiset"):
        an.validate_output(golden.source, golden.result.limit(len(golden.rows) - 1), RUN_DATE)


def test_validate_output_rejects_duplicated_row(golden):  # Thừa một dòng (nhân đôi) cũng bị từ chối.
    duplicated = golden.result.unionByName(golden.result.limit(1))

    with pytest.raises(ValueError, match="preserve the input row multiset"):
        an.validate_output(golden.source, duplicated, RUN_DATE)


def test_validate_output_rejects_altered_row_content(golden):  # Số dòng bằng nhau nhưng nội dung một cột nguồn bị đổi thì bị từ chối (so sánh multiset, không chỉ đếm).
    altered = golden.result.withColumn(
        "UnitPrice", F.when(F.col("InvoiceNo") == "10001", F.col("UnitPrice") + 1).otherwise(F.col("UnitPrice")))

    with pytest.raises(ValueError, match="preserve the input row multiset"):
        an.validate_output(golden.source, altered, RUN_DATE)


@pytest.mark.parametrize("name,transform", [
    ("wrong_run_date", lambda df: df.withColumn("run_date", F.lit("2011-12-11").cast("date"))),
    ("null_run_date", lambda df: df.withColumn("run_date", F.lit(None).cast("date"))),
    ("check_order", lambda df: df.withColumn("check_results", F.reverse("check_results"))),
    ("unknown_status", lambda df: df.withColumn("check_results", F.transform(
        "check_results", lambda c: c.withField("status", F.lit("weird"))))),
    ("unknown_tier", lambda df: df.withColumn("check_results", F.transform(
        "check_results", lambda c: c.withField("tier", F.lit("other"))))),
    ("severity_on_unflagged", lambda df: df.withColumn("check_results", F.transform(
        "check_results", lambda c: c.withField("severity", F.lit("high"))))),
    ("flag_without_severity", lambda df: df.withColumn("check_results", F.transform(
        "check_results", lambda c: c.withField("severity", F.lit(None).cast("string"))))),
    ("bad_assessment", lambda df: df.withColumn("assessment_status", F.lit("maybe"))),
    ("null_record_type", lambda df: df.withColumn("record_type", F.lit(None).cast("string"))),
    ("null_channel", lambda df: df.withColumn("channel", F.lit(None).cast("string"))),
])
def test_validate_output_rejects_malformed_checks(golden, name, transform):  # Output hỏng một cách có chủ đích (run_date sai/null, rule sai thứ tự, status/tier lạ, severity không khớp status, trạng thái đánh giá lạ, record_type/channel null) đều bị validate_output từ chối.
    with pytest.raises(ValueError, match="invalid run date, status, severity, or check order"):
        an.validate_output(golden.source, transform(golden.result), RUN_DATE)


def test_write_results_returns_resolved_partition_path_and_roundtrips(spark, golden, tmp_path):  # write_results ghi vào thư mục run_date=... (đường dẫn tuyệt đối đã resolve) và đọc lại được đúng số dòng.
    path = an.write_results(golden.result.limit(3), str(tmp_path / "out"), RUN_DATE)

    assert path == str((tmp_path / "out").resolve() / f"run_date={RUN_DATE}")
    assert spark.read.parquet(path).count() == 3


def test_write_results_overwrites_only_requested_partition(spark, golden, tmp_path):  # Ghi lại cùng ngày thì THAY THẾ partition đó (không append) và không đụng partition của ngày khác.
    root = str(tmp_path / "out")
    other = an.write_results(golden.result.limit(1), root, "2011-12-09")
    path = an.write_results(golden.result.limit(3), root, RUN_DATE)

    an.write_results(golden.result.limit(2), root, RUN_DATE)

    assert spark.read.parquet(path).count() == 2
    assert spark.read.parquet(other).count() == 1


def test_log_results_reports_counts_flags_and_not_applied(golden, caplog):  # _log_results ghi log tổng hợp: số dòng theo trạng thái/kênh/loại, các rule bị cờ (số dòng, hóa đơn, giá trị rủi ro) và lý do not_applied.
    with caplog.at_level(logging.INFO, logger="retail_anomalies"):
        an._log_results(golden.result)

    text = "\n".join(caplog.messages)
    assert "assessment_status=flagged" in text and "channel=identified" in text
    assert "record_type=sale" in text
    assert "flagged tier=business rule=quantity_deviation" in text
    assert "not_applied rule=quantity_deviation reason=insufficient_history" in text


# ---------------------------------------------------------------------------
# 8. MAIN VÀ NHÁNH --pipeline-context
# ---------------------------------------------------------------------------


class _FakeBuilder:  # Giả lập SparkSession.builder.appName().config()...getOrCreate() trả về session test.
    def __init__(self, spark):
        self._spark = spark

    def appName(self, *_):
        return self

    def config(self, *_):
        return self

    def getOrCreate(self):
        return self._spark


def patch_spark(monkeypatch, spark):  # Thay SparkSession trong module bằng session test và biến spark.stop() thành bộ đếm để không làm chết session dùng chung.
    stops = []
    monkeypatch.setattr(an, "SparkSession", SimpleNamespace(builder=_FakeBuilder(spark)))
    monkeypatch.setattr(spark, "stop", lambda: stops.append(1))
    return stops


def test_main_end_to_end_writes_validated_partition(spark, monkeypatch, tmp_path, caplog):  # main chạy end-to-end trên Parquet thật: đọc input, build, validate, ghi partition run_date=..., log tổng hợp, rồi stop Spark.
    source = tmp_path / "input"
    frame(spark, mini_rows()).write.parquet(str(source))
    stops = patch_spark(monkeypatch, spark)

    with caplog.at_level(logging.INFO, logger="retail_anomalies"):
        an.main(["--input", str(source), "--output", str(tmp_path / "out"),
                 "--run-date", RUN_DATE, "--min-samples", "5"])

    written = spark.read.parquet(str(tmp_path / "out" / f"run_date={RUN_DATE}"))
    assert written.count() == len(expected_history(mini_rows()))
    assert {"check_results", "anomaly_flags", "assessment_status"} <= set(written.columns)
    text = "\n".join(caplog.messages)
    assert "input_rows=" in text and "anomaly_rows=" in text and "assessment_status=" in text
    assert stops == [1]


def test_main_rejects_overlapping_paths_before_starting_spark(monkeypatch, tmp_path):  # Input/output chồng nhau bị chặn ngay từ đầu, trước khi tạo SparkSession.
    monkeypatch.setattr(an, "SparkSession", SimpleNamespace(
        builder=SimpleNamespace(appName=lambda *_: pytest.fail("Spark must not start"))))

    with pytest.raises(ValueError, match="overlap"):
        an.main(["--input", str(tmp_path / "data"), "--output", str(tmp_path / "data" / "out"),
                 "--run-date", RUN_DATE])


def test_main_stops_spark_when_input_contract_fails(spark, monkeypatch, tmp_path):  # Input sai kiểu dữ liệu thì main báo lỗi hợp đồng, không ghi output, và vẫn stop Spark (khối finally).
    source = tmp_path / "bad"
    frame(spark, mini_rows()).withColumn("Quantity", F.col("Quantity").cast("bigint")).write.parquet(str(source))
    stops = patch_spark(monkeypatch, spark)

    with pytest.raises(ValueError, match="Quantity has type bigint; expected int"):
        an.main(["--input", str(source), "--output", str(tmp_path / "out"), "--run-date", RUN_DATE])

    assert stops == [1] and not (tmp_path / "out").exists()


def test_main_does_not_write_when_output_validation_fails(spark, monkeypatch, tmp_path):  # build_output làm mất một dòng thì validate_output chặn và main KHÔNG ghi output nào, vẫn stop Spark.
    source = tmp_path / "input"
    frame(spark, mini_rows()).write.parquet(str(source))
    stops = patch_spark(monkeypatch, spark)
    real = an.build_output
    monkeypatch.setattr(an, "build_output", lambda *a, **k: real(*a, **k).limit(1))

    with pytest.raises(ValueError, match="preserve the input row multiset"):
        an.main(["--input", str(source), "--output", str(tmp_path / "out"), "--run-date", RUN_DATE,
                 "--min-samples", "5"])

    assert stops == [1] and not (tmp_path / "out").exists()


def test_pipeline_context_flag_delegates_to_pipeline_main(monkeypatch):  # Cờ --pipeline-context (điểm tích hợp Airflow) chuyển sang retail_pipeline.pipeline_main("audit").
    called = {}
    fake = types.ModuleType("retail_pipeline")
    fake.pipeline_main = lambda step: called.setdefault("step", step)
    monkeypatch.setitem(sys.modules, "retail_pipeline", fake)
    monkeypatch.setattr(sys, "argv", ["pyspark_anomalies.py", "--pipeline-context"])

    runpy.run_module("pyspark_anomalies", run_name="__main__")

    assert called["step"] == "audit"
