"""Test cho scripts/pyspark_clean.py (bước ETL làm sạch dữ liệu retail)."""

import runpy
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from scripts import pyspark_clean
from scripts.pyspark_clean import (
    DEFAULT_ANOMALIES_OUTPUT_PATH,
    DEFAULT_INPUT_PATH,
    DEFAULT_RFM_OUTPUT_PATH,
    TRANSACTION_SCHEMA,
    parse_args,
    prepare_datasets,
    read_transactions,
    run_cleaning_job,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_transactions.csv"
SAMPLE_TS = datetime(2026, 9, 1, 10, 0, 0)


# ---------------------------------------------------------------------------
# FIXTURE VÀ HELPER DÙNG CHUNG
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():  # Tạo MỘT SparkSession dùng chung cho cả phiên test (khởi động Spark rất chậm nên chỉ làm 1 lần), xong thì stop.
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-pyspark-clean")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def transactions(spark):  # Đọc file CSV mẫu bằng read_transactions để làm dữ liệu đầu vào cho các test cần dữ liệu "gần thật".
    return read_transactions(spark, str(FIXTURE_PATH))


def make_row(  # Helper tạo 1 dòng giao dịch hợp lệ mặc định; test chỉ cần ghi đè đúng trường muốn làm "bẩn".
    invoice="10001",
    stock="12345",
    description="Sold product",
    quantity=1,
    price=10.0,
    customer="12345",
    country="UK",
    date=SAMPLE_TS,
):
    return (invoice, stock, description, quantity, date, price, customer, country)


# ---------------------------------------------------------------------------
# 1. CLI: parse_args, main, nhánh --pipeline-context
# ---------------------------------------------------------------------------


def test_default_output_paths_follow_project_layout():  # Đảm bảo đường dẫn mặc định không bị đổi nhầm, vì các task Airflow phía sau phụ thuộc vào chúng.
    args = parse_args([])

    assert args.input == DEFAULT_INPUT_PATH == "data/raw/data.csv"
    assert args.rfm_output == DEFAULT_RFM_OUTPUT_PATH == "data/curated/RFM.parquet"
    assert (
        args.anomalies_output
        == DEFAULT_ANOMALIES_OUTPUT_PATH
        == "data/audit/anomalies.parquet"
    )


def test_parse_args_accepts_custom_paths():  # Đảm bảo đường dẫn truyền từ dòng lệnh (ví dụ trong Docker/Airflow) ghi đè được giá trị mặc định.
    args = parse_args(
        ["--input", "/opt/airflow/data/raw/in.csv",
            "--rfm-output", "/opt/airflow/data/curated/rfm_out",
            "--anomalies-output", "/opt/airflow/data/audit/anom_out",
        ]
    )

    assert args.input == "/opt/airflow/data/raw/in.csv"
    assert args.rfm_output == "/opt/airflow/data/curated/rfm_out"
    assert args.anomalies_output == "/opt/airflow/data/audit/anom_out"


def test_main_runs_job_and_always_stops_spark(monkeypatch):  # Kiểm tra main() truyền đúng tham số vào job và có stop Spark (dùng mock nên không chạy Spark thật).
    fake_spark = MagicMock(name="spark")
    fake_session_cls = MagicMock(name="SparkSession")
    fake_session_cls.builder.appName.return_value.config.return_value.getOrCreate.return_value = (
        fake_spark
    )
    fake_job = MagicMock(name="run_cleaning_job")
    monkeypatch.setattr(pyspark_clean, "SparkSession", fake_session_cls)
    monkeypatch.setattr(pyspark_clean, "run_cleaning_job", fake_job)

    pyspark_clean.main(
        ["--input", "in.csv", "--rfm-output", "rfm_out", "--anomalies-output", "anom_out"]
    )

    fake_job.assert_called_once_with(fake_spark, "in.csv", "rfm_out", "anom_out")
    fake_spark.stop.assert_called_once()


def test_main_stops_spark_even_when_job_fails(monkeypatch):  # Khi job lỗi thì Spark vẫn được stop và lỗi vẫn được ném lên để Airflow đánh dấu task FAILED.
    fake_spark = MagicMock(name="spark")
    fake_session_cls = MagicMock(name="SparkSession")
    fake_session_cls.builder.appName.return_value.config.return_value.getOrCreate.return_value = (
        fake_spark
    )
    monkeypatch.setattr(pyspark_clean, "SparkSession", fake_session_cls)
    monkeypatch.setattr(
        pyspark_clean, "run_cleaning_job", MagicMock(side_effect=RuntimeError("boom"))
    )

    with pytest.raises(RuntimeError, match="boom"):
        pyspark_clean.main([])

    fake_spark.stop.assert_called_once()


def test_pipeline_context_flag_delegates_to_pipeline_main(monkeypatch):  # Kiểm tra cờ --pipeline-context (điểm tích hợp Airflow) chuyển sang retail_pipeline.pipeline_main("etl").
    called = {}
    fake_module = types.ModuleType("retail_pipeline")
    fake_module.pipeline_main = lambda step: called.setdefault("step", step)
    monkeypatch.setitem(sys.modules, "retail_pipeline", fake_module)
    monkeypatch.setattr(sys, "argv", ["pyspark_clean.py", "--pipeline-context"])

    runpy.run_module("scripts.pyspark_clean", run_name="__main__")

    assert called["step"] == "etl"


# ---------------------------------------------------------------------------
# 2. ĐỌC DỮ LIỆU: read_transactions
# ---------------------------------------------------------------------------


def test_read_transactions_uses_stable_business_types(transactions):  # Đảm bảo dữ liệu đọc ra đúng schema cố định (CustomerID là chuỗi, InvoiceDate là timestamp...) để không bị "trôi" kiểu.
    expected_types = {"CustomerID": StringType,
        "InvoiceNo": StringType,
        "InvoiceDate": TimestampType,
        "Quantity": IntegerType,
        "UnitPrice": DoubleType,
    }

    assert transactions.schema == TRANSACTION_SCHEMA
    for column, expected_type in expected_types.items():
        assert isinstance(transactions.schema[column].dataType, expected_type)
    assert transactions.filter(F.col("InvoiceDate").isNull()).count() == 0
    assert transactions.filter(F.col("CustomerID").isNull()).count() == 2


def test_read_transactions_parses_timestamp_format_and_nan(spark, tmp_path):  # Kiểm tra ngày giờ "12/1/2010 8:26" được parse đúng giá trị và chuỗi "NaN" được hiểu là null.
    csv_path = tmp_path / "mini.csv"
    csv_path.write_text(
        "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
        "536365,85123A,WHITE HANGING HEART,6,12/1/2010 8:26,2.55,17850,United Kingdom\n"
        "536366,22633,HAND WARMER,6,12/1/2010 8:28,1.85,NaN,United Kingdom\n"
    )

    df = read_transactions(spark, str(csv_path))
    rows = {
        r.InvoiceNo: r
        for r in df.select(
            "InvoiceNo",
            "CustomerID",
            F.date_format("InvoiceDate", "yyyy-MM-dd HH:mm:ss").alias("ts"),
        ).collect()
    }

    assert rows["536365"].ts == "2010-12-01 08:26:00"
    assert rows["536365"].CustomerID == "17850"
    assert rows["536366"].CustomerID is None


# ---------------------------------------------------------------------------
# 3. LOGIC LÀM SẠCH: prepare_datasets
# ---------------------------------------------------------------------------


def test_prepare_datasets_applies_each_output_rule(transactions):  # Test tổng hợp trên file mẫu: anomalies chỉ bỏ dòng trùng, còn rfm phải sạch (không null, không hóa đơn hủy, số lượng/giá > 0).
    rfm, anomalies = prepare_datasets(transactions)

    assert transactions.count() == 8
    assert anomalies.count() == 7
    assert rfm.count() == 1
    assert anomalies.filter(F.col("CustomerID").isNull()).count() == 2
    assert rfm.filter(F.col("CustomerID").isNull()).count() == 0
    assert rfm.filter(F.upper(F.col("InvoiceNo")).startswith("C")).count() == 0
    assert rfm.filter(F.col("Quantity") <= 0).count() == 0
    assert rfm.filter(F.col("UnitPrice") <= 0).count() == 0


def test_prepare_datasets_drops_exact_duplicates_only(spark):  # Kiểm tra chỉ dòng trùng HỆT NHAU mới bị xóa, dòng chỉ giống một phần thì giữ lại.
    row_a = make_row(invoice="1")
    row_b = make_row(invoice="2")
    df = spark.createDataFrame([row_a, row_a, row_b], schema=TRANSACTION_SCHEMA)

    rfm, anomalies = prepare_datasets(df)

    assert anomalies.count() == 2
    assert {r.InvoiceNo for r in rfm.collect()} == {"1", "2"}


def test_prepare_datasets_removes_cancelled_invoices_from_rfm_only(spark):  # Kiểm tra hóa đơn hủy (bắt đầu bằng "C", kể cả chữ thường/có khoảng trắng) bị loại khỏi RFM nhưng vẫn còn trong anomalies.
    rows = [
        make_row(invoice="C10001"),
        make_row(invoice="c10002"),
        make_row(invoice=" C10003 "),
        make_row(invoice="10004"),
    ]
    df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)

    rfm, anomalies = prepare_datasets(df)

    assert [r.InvoiceNo for r in rfm.collect()] == ["10004"]
    assert anomalies.count() == 4


def test_prepare_datasets_quantity_and_price_boundaries(spark):  # Kiểm tra giá trị biên: Quantity và UnitPrice phải LỚN HƠN 0 (bằng 0 hoặc âm đều bị loại).
    rows = [
        make_row(invoice="1", quantity=0),
        make_row(invoice="2", quantity=-5),
        make_row(invoice="3", price=0.0),
        make_row(invoice="4", price=-1.0),
        make_row(invoice="5", quantity=1, price=0.01),
    ]
    df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)

    rfm, anomalies = prepare_datasets(df)

    assert [r.InvoiceNo for r in rfm.collect()] == ["5"]
    assert anomalies.count() == 5


def test_prepare_datasets_drops_rows_with_any_null_from_rfm(spark):  # Ghi lại hành vi: dòng có BẤT KỲ cột nào null (CustomerID, Description, InvoiceDate) đều bị loại khỏi RFM.
    rows = [
        make_row(invoice="1", customer=None),
        make_row(invoice="2", description=None),
        make_row(invoice="3", date=None),
        make_row(invoice="4"),
    ]
    df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)

    rfm, anomalies = prepare_datasets(df)

    assert [r.InvoiceNo for r in rfm.collect()] == ["4"]
    assert anomalies.count() == 4


def test_prepare_datasets_excludes_service_lines_only_from_rfm(spark):  # Kiểm tra các dòng phí dịch vụ (POST, BANK CHARGES, PACKING CHARGE...) bị loại khỏi RFM, nhưng hàng thật có chữ "carriage" thì giữ.
    rows = [
        make_row(invoice="10001", stock="12345", description="Sold product"),
        make_row(invoice="10002", stock=" POST ", description="Postage"),
        make_row(invoice="10003", stock="m", description="Manual"),
        make_row(invoice="10004", stock="DOT", description="Dotcom Postage"),
        make_row(invoice="10005", stock="BANK CHARGES", description="Bank Charges"),
        make_row(invoice="10006", stock="C2", description="Carriage"),
        make_row(invoice="10007", stock="PADS", description="Pads to match all cushions"),
        make_row(invoice="10008", stock="23574", description=" packing charge "),
        make_row(invoice="10009", stock="23444", description="Next Day Carriage"),
        make_row(invoice="10010", stock="23099", description="French carriage lantern"),
    ]
    df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)

    rfm, anomalies = prepare_datasets(df)

    assert {row.StockCode for row in rfm.select("StockCode").collect()} == {
        "12345",
        "23099",
    }
    assert anomalies.count() == len(rows)


# ---------------------------------------------------------------------------
# 4. GHI FILE: run_cleaning_job
# ---------------------------------------------------------------------------


def test_run_cleaning_job_writes_both_parquet_outputs(spark, tmp_path):  # Test end-to-end: chạy cả job đọc -> làm sạch -> ghi 2 thư mục Parquet (vào thư mục tạm), rồi đọc lại kiểm tra số dòng và schema.
    rfm_output = tmp_path / "curated" / "RFM.parquet"
    anomalies_output = tmp_path / "audit" / "anomalies.parquet"

    returned_rfm, returned_anomalies = run_cleaning_job(
        spark,
        str(FIXTURE_PATH),
        str(rfm_output),
        str(anomalies_output),
    )

    assert rfm_output.is_dir()
    assert anomalies_output.is_dir()

    written_rfm = spark.read.parquet(str(rfm_output))
    written_anomalies = spark.read.parquet(str(anomalies_output))

    assert written_rfm.count() == 1
    assert written_anomalies.count() == 7
    assert written_rfm.schema == TRANSACTION_SCHEMA
    assert written_anomalies.schema == TRANSACTION_SCHEMA
    assert returned_rfm.count() == written_rfm.count()
    assert returned_anomalies.count() == written_anomalies.count()


def test_run_cleaning_job_is_idempotent_on_rerun(spark, tmp_path):  # Kiểm tra chạy job 2 lần liên tiếp (như Airflow retry) vẫn ra kết quả y hệt: ghi đè chứ không lỗi hay nhân đôi dữ liệu.
    rfm_output = str(tmp_path / "RFM.parquet")
    anomalies_output = str(tmp_path / "anomalies.parquet")

    run_cleaning_job(spark, str(FIXTURE_PATH), rfm_output, anomalies_output)
    run_cleaning_job(spark, str(FIXTURE_PATH), rfm_output, anomalies_output)

    assert spark.read.parquet(rfm_output).count() == 1
    assert spark.read.parquet(anomalies_output).count() == 7
