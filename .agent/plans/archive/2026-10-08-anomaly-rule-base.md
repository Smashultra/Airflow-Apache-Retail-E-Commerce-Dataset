# Rule base anomaly hai cấp theo kênh và theo khách hàng — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

- Ngày: 2026-10-08. Trạng thái: hoàn thành trên nhánh feature/anomaly-rule-base; kết quả và đề xuất hiệu chỉnh ở docs/ANOMALY_DETECTION_VI.md mục 9.
- Owner: Claude. Nhánh: `feature/anomaly-rule-base` (tạo từ `main` trước Task 1).
- Nguồn yêu cầu: trao đổi ngày 2026-10-08 — tách kênh (DOT/web, lẻ khác, nghiệp vụ kho), hai cấp data/business anomaly dựa trên chuẩn kiểm chứng, baseline theo từng khách định danh, chuyển `assess_orders` sang `pyspark_anomalies.py`, ghi toàn bộ quá trình vào một file markdown.

**Goal:** Thay bộ 9 check hiện tại (6 nghiệp vụ + 3 IQR toàn cục theo sản phẩm) và ngưỡng hóa đơn mean+3σ bằng một rule base hai cấp, phân loại bản ghi/kênh trước, dùng baseline robust phân cấp (khách × sản phẩm → khách → kênh × sản phẩm), có severity và giá trị ảnh hưởng (GBP).

**Architecture:** Toàn bộ logic nằm trong `scripts/pyspark_anomalies.py` (DAG gọi qua `--pipeline-context`, `retail_pipeline.run_audit` chỉ import và ghi output). Pipeline trong job: base columns → invoice summary → `record_type` + `channel` → Tier 1 data rules → Tier 2 business rules → tổng hợp dòng; `assess_orders` chạy riêng ở cấp hóa đơn. Một script bằng chứng chạy pandas trên host sinh bảng số liệu cho tài liệu `docs/ANOMALY_DETECTION_VI.md`.

**Tech Stack:** PySpark 4.2.0 (Docker image `ecommerce-airflow:3.3.1`), pandas trên host cho script bằng chứng, pytest.

**Spec:** phần trao đổi trong session 2026-10-08 được chốt trong mục *Đặc tả rule base* bên dưới; file này là đặc tả duy nhất.

## Global Constraints

- Giữ nguyên multiset dòng đầu vào trước cutoff (`validate_output` hiện có); không xóa, không sửa, không điền CustomerID.
- Hồi cứu: chỉ dùng dòng có `InvoiceDate < run_date` (và dòng thiếu ngày cho rule áp dụng được), giống `select_history` hiện có. Baseline in-sample, ghi rõ trong tài liệu.
- Thiếu CustomerID **không** phải data anomaly; nó là thuộc tính kênh.
- Ngưỡng thống kê: modified z-score Iglewicz–Hoaglin trên thang log, `Z_THRESHOLD = 3.5`, `MAD_CONSTANT = 0.6745`.
- Ngưỡng trọng yếu (tolerable difference theo tinh thần ISA 520, đặt trước khi xem kết quả): `QUANTITY_MIN_FOLD = 3.0`, `PRICE_MIN_RELATIVE = 0.30`, `ORDER_MIN_FOLD = 3.0`.
- Số mẫu tối thiểu: `MIN_CUSTOMER_STOCK_LINES = 5`, `MIN_CUSTOMER_LINES = 20`, `MIN_CUSTOMER_INVOICES = 5`, `MIN_CHANNEL_SAMPLES = 30` (= `--min-samples` mặc định).
- Severity theo `value_at_risk`: `high` ≥ 1,000 GBP, `medium` ≥ 100 GBP, còn lại `low`; ngoại lệ ghi trong bảng rule.
- Mọi hằng số trên chỉ được đổi ở Task 1 nếu bằng chứng cho thấy không hợp lý, và phải được người dùng duyệt trước khi code Task 4.
- Flag là **ứng viên cần xem xét**, không phải kết luận gian lận/lỗi.
- Không commit dữ liệu, Parquet sinh ra, `.env`. Không push khi chưa được yêu cầu.
- Lệnh test chuẩn (Docker): `docker compose run --rm --no-deps airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_pyspark_anomalies.py tests/test_retail_pipeline.py`

## Đặc tả rule base

### Tầng 0 — phân loại (không phải anomaly)

`record_type` (theo thứ tự ưu tiên, loại trừ nhau):

| # | record_type | Điều kiện (sau trim/upper) |
|---|---|---|
| 1 | `accounting_adjustment` | InvoiceNo bắt đầu `A`, hoặc StockCode = `B`, hoặc Description = `ADJUST BAD DEBT` |
| 2 | `cancellation` | InvoiceNo bắt đầu `C` |
| 3 | `fee_service` | `__service` hoặc `__special` hiện có (POST, DOT, M, BANK CHARGES, AMAZONFEE, CRUK, …) |
| 4 | `inventory_adjustment` | UnitPrice = 0, thiếu CustomerID, hóa đơn không có dòng UnitPrice > 0 |
| 5 | `sale` | Quantity > 0, UnitPrice > 0, line_value hữu hạn > 0 |
| 6 | `zero_price_line` | UnitPrice = 0, Quantity > 0 (còn lại sau #4) |
| 7 | `unclassified` | còn lại (giá trị thiếu/không hợp lệ, …) |

`channel` (cấp hóa đơn, gán cho mọi dòng của hóa đơn):

| channel | Điều kiện |
|---|---|
| `identified` | hóa đơn có ≥ 1 dòng có CustomerID (tập khách chủ yếu là sỉ theo mô tả UCI) |
| `retail_web` | không có ID và hóa đơn có dòng StockCode `DOT` |
| `retail_other` | không có ID, không DOT, có dòng UnitPrice > 0 không phải `accounting_adjustment` |
| `internal` | không có ID, không có dòng nào thỏa trên (chỉ kiểm kho/bút toán) |
| `unknown` | thiếu InvoiceNo |

Cột phụ `has_dotcom_postage` (bool, cấp hóa đơn). Baseline lẻ gộp `retail_web` + `retail_other` thành nhóm `retail`; baseline định danh dùng nhóm `identified`.

### Tầng 1 — Data anomaly (ISO/IEC 25012 / DAMA)

| Rule | Chiều chất lượng | Điều kiện flag | Áp dụng | Severity |
|---|---|---|---|---|
| `missing_required_field` | Completeness | thiếu InvoiceNo/StockCode/InvoiceDate/Quantity/UnitPrice/Country | mọi dòng | high |
| `missing_description` | Completeness | Description thiếu | mọi dòng | low |
| `invalid_numeric_value` | Validity | Quantity null hoặc UnitPrice/line_value không hữu hạn | mọi dòng | high |
| `cancel_sign_conflict` | Consistency | `cancellation` và Quantity ≥ 0 | có InvoiceNo, Quantity hợp lệ | medium |
| `negative_price_outside_adjustment` | Validity | UnitPrice < 0 và record_type ≠ `accounting_adjustment` | giá hợp lệ | high |
| `invoice_multiple_customers` | Consistency | hóa đơn có ≥ 2 CustomerID | có InvoiceNo, (mọi dòng có ID hoặc ≥ 2 ID) | medium |
| `invoice_inconsistent_header` | Consistency | hóa đơn có > 1 Country hoặc > 1 ngày (`to_date(InvoiceDate)`) | có InvoiceNo | medium |

Trùng lặp (Uniqueness) đã bị loại ở `pyspark_clean.prepare_datasets` (5,268 dòng); chỉ ghi nhận trong tài liệu, không có rule dòng.

### Tầng 2 — Business anomaly (Chandola 2009: point / contextual / collective)

Ký hiệu: `x = ln(value)`, `med`, `MAD` của baseline trên thang log, `z = 0.6745·(x − med)/MAD`, `fold = exp(x − med)`. Khi `MAD = 0`: `robust_z = null`, điều kiện thống kê coi là thỏa nếu `x ≠ med` theo đúng chiều; ngưỡng trọng yếu quyết định.

| Rule | Loại | Áp dụng | Baseline (cấp đầu tiên đủ mẫu) | Flag khi | value_at_risk |
|---|---|---|---|---|---|
| `quantity_deviation` | contextual | `sale` | identified: `customer_stock` (ln Quantity, n ≥ 5) → `customer` (ln(Quantity / median Quantity của stock trong nhóm identified), n ≥ 20) → `channel_stock` (n ≥ 30); retail: `channel_stock` | z > 3.5 và fold ≥ 3.0 | (Quantity − expected_qty) × UnitPrice |
| `price_deviation` | contextual | `sale` | identified: `customer_stock` (n ≥ 5) → `channel_stock` (n ≥ 30); retail: `channel_stock` | \|z\| > 3.5 và \|fold − 1\| ≥ 0.30 | \|UnitPrice − expected_price\| × Quantity |
| `zero_price_sale_line` | point | `zero_price_line` trong hóa đơn có dòng giá dương | — | luôn | Quantity × median giá bán của stock (mọi kênh) |
| `all_zero_price_invoice` | collective | `zero_price_line` của hóa đơn có ID mà mọi dòng giá 0 | — | luôn | như trên |
| `inventory_adjustment` | point | `inventory_adjustment` | — | luôn | \|Quantity\| × median giá bán của stock (null nếu không có) → severity `low` khi null |
| `manual_accounting_entry` | point | `accounting_adjustment` | — | luôn | \|line_value\|; severity luôn `high` |
| `unmatched_return` | collective | `cancellation` có ID | tổng theo customer × stock trong lịch sử | \|Σ Quantity hủy\| > Σ Quantity `sale` | \|line_value\| |

Lý do `not_applied` chuẩn: `outside_scope`, `missing_invoice_no`, `invalid_numeric_value`, `insufficient_history`, `no_customer_history`.

Cấp hóa đơn (`assess_orders`, chuyển từ `retail_pipeline.py`): `order_value_deviation` — tổng hóa đơn đủ điều kiện (giữ nguyên tiêu chí eligibility hiện có) so với baseline `customer` (≥ 5 hóa đơn của khách) → `channel` (nhóm identified hoặc retail, ≥ 30 hóa đơn); flag khi z > 3.5 và fold ≥ 3.0.

### Check struct (thay struct cũ)

`tier` (`data`|`business`), `rule`, `status` (`flagged`|`not_flagged`|`not_applied`), `reason`, `context_level` (null|`customer_stock`|`customer`|`channel_stock`|`channel`), `observed_value`, `reference_count` (long), `reference_median` (double, đơn vị gốc = `exp(med)`), `reference_mad` (double, thang log), `robust_z` (double), `fold_change` (double), `value_at_risk` (double), `severity` (null khi không flagged).

Cột dòng mới: `record_type`, `channel`, `has_dotcom_postage`, `data_anomaly_flags`, `business_anomaly_flags`, `anomaly_flags` (= nối hai mảng), `max_severity`, `assessment_status` (giữ ngữ nghĩa cũ). Bỏ `iqr_multiplier`; thêm `z_threshold`. Bỏ `context_flags` từ khóa? **Không** — giữ `context_flags` hiện có (dùng làm bằng chứng mô tả).

## Review Focus

1. Customer chỉ có hóa đơn hủy (không có dòng `sale`) → `unmatched_return` flag mọi dòng hủy, không lỗi chia/0. Test ở Task 5.
2. Baseline MAD = 0 (giá cố định của một khách) với một dòng giá khác 40% → flagged, `robust_z` null, `fold_change` đúng. Test ở Task 4.
3. Hóa đơn thiếu ID có cả DOT và dòng hủy/giá 0 → channel `retail_web` cho mọi dòng, dòng giá 0 thành `zero_price_line` chứ không phải `inventory_adjustment`. Test ở Task 2.
4. StockCode chỉ xuất hiện trong kênh retail khi đánh giá dòng identified → rơi xuống `insufficient_history`, không dùng baseline kênh khác. Test ở Task 4.
5. Dữ liệu rỗng / chỉ toàn dòng sau cutoff → output rỗng đúng schema, `assess_orders` không lỗi khi stats null. Test ở Task 6 và Task 7.

---

### Task 1: Bằng chứng dữ liệu và khung tài liệu

**Files:**
- Create: `scripts/anomaly_evidence.py`
- Create: `tests/test_anomaly_evidence.py`
- Create: `docs/ANOMALY_DETECTION_VI.md`

**Interfaces:**
- Produces: `load_dedup(path: str) -> pandas.DataFrame` (đọc CSV ISO-8859-1, CustomerID/InvoiceNo/StockCode dạng str, drop trùng 8 cột); `invoice_channels(frame) -> pandas.Series` (index InvoiceNo, giá trị theo bảng channel ở trên); `main(argv) -> None` in các bảng E1–E8 dạng markdown ra stdout.

- [ ] **Step 1: Tạo nhánh** `git switch -c feature/anomaly-rule-base`; ghi owner/branch vào `.agent/TODO.md`.
- [ ] **Step 2: Viết test** `test_invoice_channels_priority`: 4 hóa đơn fixture — có ID + DOT → `identified`; thiếu ID + DOT → `retail_web`; thiếu ID giá dương → `retail_other`; thiếu ID chỉ giá 0 → `internal`. Dùng `pytest.importorskip("pandas")`.
- [ ] **Step 3: Chạy** `python -m pytest -q tests/test_anomaly_evidence.py` trên host → FAIL (module chưa có).
- [ ] **Step 4: Implement** `load_dedup`, `invoice_channels`, và các bảng:
  - E1 DOT: số hóa đơn/dòng/giá trị DOT theo có/thiếu ID.
  - E2 Kênh: hóa đơn, dòng, giá trị dòng dương theo `channel`.
  - E3 Hồ sơ `sale` theo kênh: mode/quantile Quantity, mean/quantile UnitPrice; median tỷ lệ giá retail/identified trên cùng StockCode (chỉ stock có ≥ 30 dòng mỗi bên).
  - E4 `record_type` × `channel` (cài lại thứ tự ưu tiên bằng pandas).
  - E5 Độ sâu lịch sử identified: phân phối số hóa đơn/khách; % dòng `sale` có baseline ở từng cấp với ngưỡng 5/20/30.
  - E6 Phân phối value_at_risk ước tính của `inventory_adjustment` (quantile 50/90/99, số dòng ≥ 100 / ≥ 1,000 GBP).
  - E7 % cặp customer × stock (n ≥ 5) có MAD log giá = 0.
  - E8 Số cặp customer × stock có hủy vượt mua.
- [ ] **Step 5: Chạy test** → PASS. Chạy `python -I scripts/anomaly_evidence.py --input data/raw/data.csv` → in đủ E1–E8.
- [ ] **Step 6: Viết khung `docs/ANOMALY_DETECTION_VI.md`** với các mục: 1. Mục tiêu & phạm vi; 2. Chuẩn tham chiếu (ISO/IEC 25012, DAMA-DMBOK, Chandola 2009, Iglewicz–Hoaglin/NIST, ISA 520, mô tả UCI "non-store online retail") kèm URL; 3. Tóm tắt EDA anomaly đã có (từ `notebooks/EDA_Anomalies_Guide_VI.md`, `notebooks/EDA_Online_Retail_Guide.md`); 4. Bằng chứng mới E1–E8 (dán bảng); 5. Insight (kênh, kiểm kho, giá lẻ); 6. Rule base (bảng Tầng 0/1/2 ở trên + logic); 7. Tham khảo đã cân nhắc (repo Tracy-yyq: Isolation Forest, contamination 5% cố định trên dữ liệu khác — không áp dụng vì ép tỷ lệ bất thường và không giải thích được từng rule); 8. Nhật ký triển khai (mỗi task một dòng: bước, kết quả, lệnh kiểm chứng); 9. Kết quả chạy đầy đủ (điền ở Task 8); 10. Giới hạn.
- [ ] **Step 7: Điểm dừng duyệt.** Nếu E3/E5/E6/E7 cho thấy hằng số ở Global Constraints không hợp lý (ví dụ < 50% dòng identified có baseline nào), đề xuất giá trị mới kèm bảng cho người dùng; chỉ tiếp Task 2 sau khi được duyệt.
- [ ] **Step 8: Commit** `scripts/anomaly_evidence.py tests/test_anomaly_evidence.py docs/ANOMALY_DETECTION_VI.md .agent/TODO.md` — "Add anomaly evidence script and documentation skeleton".

### Task 2: Tầng 0 — record_type và channel trong Spark

**Files:**
- Modify: `scripts/pyspark_anomalies.py` (thay `add_invoice_checks` bằng `add_invoice_summary` + `add_record_context`)
- Test: `tests/test_pyspark_anomalies.py`

**Interfaces:**
- Consumes: `add_base_columns`, `add_description_context` hiện có.
- Produces: `add_invoice_summary(frame: DataFrame) -> DataFrame` (thêm `__invoice_lines`, `__priced_lines`, `__positive_price_lines`, `__zero_price_lines`, `__customer_count`, `__identified_lines`, `__country_count`, `__date_count`, `has_dotcom_postage`); `add_record_context(frame: DataFrame) -> DataFrame` (thêm `record_type`, `channel`, `__baseline_group` ∈ {`identified`,`retail`,null}).

- [ ] **Step 1: Viết test** `test_record_type_priority_and_channels`: các dòng fixture cho đủ 7 record_type và 5 channel theo bảng đặc tả, gồm case Review Focus #3; assert từng `(InvoiceNo, StockCode) → (record_type, channel)`.
- [ ] **Step 2: Chạy** lệnh test chuẩn với `-k record_type` → FAIL.
- [ ] **Step 3: Implement** hai hàm; `has_dotcom_postage` = hóa đơn có `__stock == "DOT"`.
- [ ] **Step 4: Chạy test** → PASS.
- [ ] **Step 5: Ghi nhật ký** vào mục 8 của tài liệu; **Commit** "Classify anomaly rows by record type and channel".

### Task 3: Tầng 1 — data rules, check struct mới, severity

**Files:**
- Modify: `scripts/pyspark_anomalies.py`
- Test: `tests/test_pyspark_anomalies.py`

**Interfaces:**
- Produces: `DATA_RULES: tuple[str, ...]` (thứ tự như bảng Tầng 1); `_check(tier: str, rule: str, reason: Column, flagged: Column, *, observed=None, context_level=None, reference_count=None, reference_median=None, reference_mad=None, robust_z=None, fold_change=None, value_at_risk=None, severity: Column | None = None) -> Column`; `_severity_from_value(value_at_risk: Column) -> Column`; `add_data_checks(frame) -> DataFrame` (thêm `__data_checks`).

- [ ] **Step 1: Viết test** `test_data_rules_dimensions_and_severity`: mỗi rule một dòng vi phạm và một dòng sạch; assert `status`, `tier == "data"`, severity đúng bảng; dòng thiếu CustomerID sạch có toàn bộ data rule `not_flagged`; bad debt (`A563186`, giá −11,062.06) **không** bị `negative_price_outside_adjustment`.
- [ ] **Step 2: Chạy** → FAIL.
- [ ] **Step 3: Implement** theo bảng; `_severity_from_value` dùng ngưỡng 1,000/100.
- [ ] **Step 4: Chạy** → PASS.
- [ ] **Step 5: Ghi nhật ký; Commit** "Add tier-1 data anomaly rules".

### Task 4: Baseline robust phân cấp — quantity_deviation, price_deviation

**Files:**
- Modify: `scripts/pyspark_anomalies.py` (xóa `add_product_iqr_checks`, `IQR_RULES`)
- Test: `tests/test_pyspark_anomalies.py`

**Interfaces:**
- Produces: `robust_baseline(frame: DataFrame, keys: list[str], log_value: Column, prefix: str) -> DataFrame` trả `keys + [f"{prefix}_n", f"{prefix}_med", f"{prefix}_mad"]` (median và MAD chính xác bằng `percentile(…, 0.5)` hai lượt); `add_deviation_checks(frame, z_threshold: float, min_samples: int) -> DataFrame` (thêm `__deviation_checks` = [quantity_deviation, price_deviation]).

- [ ] **Step 1: Viết test** `test_quantity_hierarchy_levels`: khách A có 6 dòng stock X quantity 10 + 1 dòng 40 → flagged, `context_level == "customer_stock"`, `fold_change == 4.0`; khách B chỉ 2 dòng stock X nhưng 25 dòng stock khác → `customer`; khách C mới → `channel_stock`; dòng retail luôn `channel_stock`; dòng 25 (fold 2.5) **không** flag dù z lớn.
- [ ] **Step 2: Viết test** `test_price_deviation_zero_mad_and_direction`: Review Focus #2 (giá 1.00 × 5, dòng 1.40 → flagged, `robust_z is None`; dòng 1.20 → không flag vì < 30%; dòng 0.60 → flagged chiều giảm).
- [ ] **Step 3: Viết test** `test_identified_never_uses_retail_baseline`: Review Focus #4.
- [ ] **Step 4: Chạy** → FAIL.
- [ ] **Step 5: Implement.** Baseline chỉ dựng từ `record_type == "sale"` có `InvoiceDate` không null; nhóm theo `__baseline_group`. Chọn cấp bằng `coalesce` theo thứ tự ưu tiên khi `n ≥` ngưỡng của cấp.
- [ ] **Step 6: Chạy** → PASS.
- [ ] **Step 7: Ghi nhật ký; Commit** "Replace product IQR with hierarchical robust deviation checks".

### Task 5: Các business rule nghiệp vụ còn lại

**Files:**
- Modify: `scripts/pyspark_anomalies.py`
- Test: `tests/test_pyspark_anomalies.py`

**Interfaces:**
- Produces: `BUSINESS_RULES: tuple[str, ...]` = `("quantity_deviation", "price_deviation", "zero_price_sale_line", "all_zero_price_invoice", "inventory_adjustment", "manual_accounting_entry", "unmatched_return")`; `add_operational_checks(frame) -> DataFrame` (thêm `__operational_checks` cho 5 rule sau).

- [ ] **Step 1: Viết test** `test_operational_rules_value_at_risk`: dòng kiểm kho Quantity −20 stock có median giá 2.0 → `inventory_adjustment` flagged, `value_at_risk == 40.0`, severity `low`; stock không có lịch sử bán → value null, severity `low`; bad debt → `manual_accounting_entry` severity `high`; giá 0 trong hóa đơn trả tiền → `zero_price_sale_line`; hóa đơn có ID toàn giá 0 → `all_zero_price_invoice`.
- [ ] **Step 2: Viết test** `test_unmatched_return_collective`: khách mua 5 hủy 8 → flagged mọi dòng hủy cặp đó; khách mua 10 hủy 3 → không flag; Review Focus #1 (chỉ có hủy); hủy thiếu ID → `not_applied`, reason `no_customer_history`.
- [ ] **Step 3: Chạy** → FAIL. **Step 4: Implement.** **Step 5: Chạy** → PASS.
- [ ] **Step 6: Ghi nhật ký; Commit** "Add operational business anomaly rules".

### Task 6: Lắp ráp build_output, validate, CLI, log

**Files:**
- Modify: `scripts/pyspark_anomalies.py` (`parse_args`, `build_output`, `validate_output`, `_log_results`, `main`)
- Modify: `README.md` (đoạn *Retrospective anomaly checks* và lệnh mẫu)
- Test: `tests/test_pyspark_anomalies.py` (viết lại các test IQR cũ; giữ test path/partition)

**Interfaces:**
- Produces: `RULES = DATA_RULES + BUSINESS_RULES`; `build_output(transactions, run_date, z_threshold: float = 3.5, min_samples: int = 30) -> DataFrame`; CLI `--z-threshold` (mặc định 3.5, hữu hạn > 0) thay `--iqr-multiplier`; `--min-samples` giữ nguyên.

- [ ] **Step 1: Sửa test** `test_cli_rejects_bad_parameters_and_overlapping_paths` cho `--z-threshold`; thêm `test_output_schema_rule_order_and_summary` (thứ tự `check_results` == `RULES`, `anomaly_flags` == data + business flags, `max_severity` là mức cao nhất, `assessment_status` như cũ); Review Focus #5 phần dòng trong `test_empty_and_partition_round_trip`.
- [ ] **Step 2: Chạy** → FAIL. **Step 3: Implement.** `_log_results` log thêm theo `tier`, `channel`, `severity`.
- [ ] **Step 4: Chạy toàn bộ** `tests/test_pyspark_anomalies.py` → PASS.
- [ ] **Step 5: Ghi nhật ký; Commit** "Assemble two-tier anomaly output and CLI".

### Task 7: Chuyển assess_orders sang pyspark_anomalies.py

**Files:**
- Modify: `scripts/pyspark_anomalies.py` (thêm `assess_orders`)
- Modify: `scripts/retail_pipeline.py:110-200` (xóa `assess_orders`, `import math` nếu không còn dùng), `scripts/retail_pipeline.py:338-345` (import từ `pyspark_anomalies`)
- Test: chuyển `test_order_total_missing_customer_and_invalid_line`, `test_sparse_or_constant_reference_is_not_a_negative_finding` từ `tests/test_retail_pipeline.py` sang `tests/test_pyspark_anomalies.py` và viết lại

**Interfaces:**
- Produces: `assess_orders(transactions: DataFrame, run_date: str, z_threshold: float = 3.5, min_samples: int = 30) -> DataFrame` — một dòng/hóa đơn: `invoice_key`, `channel`, `customer_key`, `line_count`, `invalid_lines`, `customer_ids`, `countries`, `invoice_days`, `eligible`, `order_total`, `reason`, `context_level`, `reference_count`, `reference_median`, `reference_mad`, `robust_z`, `fold_change`, `value_at_risk`, `severity`, `currency`, `z_threshold`, `run_date`, `reference_mode`, `status`. Giữ nguyên tiêu chí eligibility và các reason `cancelled_invoice`, `invalid_invoice_lines`, `multiple_customer_ids`, `inconsistent_country`, `inconsistent_invoice_date`; thay `insufficient_reference`/`zero_or_invalid_stddev` bằng `insufficient_history`.

- [ ] **Step 1: Viết test** `test_order_value_customer_then_channel_baseline`: khách có 6 hóa đơn ~100 GBP và 1 hóa đơn 450 → flagged `customer`; khách 1 hóa đơn → baseline `channel`; hóa đơn retail chỉ so với retail; hóa đơn `BAD`/`C1` giữ reason cũ; Review Focus #5 (frame rỗng → 0 dòng, không lỗi).
- [ ] **Step 2: Chạy** → FAIL. **Step 3: Implement** (dùng lại `robust_baseline` của Task 4).
- [ ] **Step 4: Sửa `run_audit`** gọi `assess_orders(source, context["run_date"])` từ `pyspark_anomalies`; giữ ba output `row_assessments`, `order_assessments`, `flagged_orders` và các kiểm tra reconcile.
- [ ] **Step 5: Chạy** lệnh test chuẩn + `tests/test_retail_dag.py tests/test_retail_contracts.py` → PASS.
- [ ] **Step 6: Ghi nhật ký; Commit** "Move invoice-level assessment into anomaly job with robust baselines".

### Task 8: Chạy dữ liệu đầy đủ, hoàn thiện tài liệu, bàn giao

**Files:**
- Modify: `docs/ANOMALY_DETECTION_VI.md` (mục 9, 10), `.agent/DECISIONS.md`, `.agent/HANDOFF.md`, `.agent/TODO.md`
- Move: plan này sang `.agent/plans/archive/`

- [ ] **Step 1: Chạy clean + anomaly kiểu cũ** trong Docker (lệnh trong README, `--driver-memory 3g`), sau đó chạy DAG stage audit nếu stack Airflow đã lên (theo `docs/SETUP.md`); ghi exit code.
- [ ] **Step 2: Đối soát** bằng Spark: số dòng output = 536,641 (hoặc số history trước cutoff); bảng `record_type × channel` khớp E4 của Task 1 (lệch phải giải thích).
- [ ] **Step 3: Điền mục 9** của tài liệu: flagged theo tier/rule/channel/severity/context_level; tổng `value_at_risk`; số hóa đơn flagged mới so với 133 của mean+3σ cũ; 3–5 ví dụ minh họa mỗi rule business.
- [ ] **Step 4: DECISIONS.md** thêm mục quyết định (hai tầng, chuẩn, hằng số, thay IQR/mean+3σ); ghi chú các chỗ handbook (`docs/handbook/*`, `docs/DAG_DESIGN_VI.md`) còn mô tả "chín row checks"/mean+3σ là đã lỗi thời.
- [ ] **Step 5: Chạy toàn bộ test Docker** (lệnh trong README) và `python -m pytest -q tests/test_pyspark_jobs.py tests/test_anomaly_evidence.py` trên host; ghi kết quả vào HANDOFF.
- [ ] **Step 6: Archive plan; Commit** "Document anomaly rule base results and hand off".

## Acceptance criteria

- Mọi dòng trong history có đúng một `record_type`, một `channel`, đủ 14 check theo thứ tự `RULES`.
- Không dòng thiếu CustomerID nào bị flag chỉ vì thiếu ID.
- Mỗi flag business có `context_level` (nếu thống kê), `value_at_risk` hoặc lý do null, và `severity`.
- `retail_pipeline.py` không còn logic anomaly; DAG vẫn ghi ba output và manifest hợp lệ.
- `docs/ANOMALY_DETECTION_VI.md` chứa đủ 10 mục, mọi con số có lệnh tái tạo.

## Risks and rollback

- Tốn tài nguyên: hai lượt percentile theo customer × stock trên ~400k dòng; nếu OOM với 3 GB, tăng `spark.sql.shuffle.partitions` trước khi đổi thuật toán.
- Hằng số có thể sinh quá nhiều flag (đặc biệt `inventory_adjustment` luôn flag ~2k dòng): đó là chủ ý (sổ cần đối soát), severity giúp lọc.
- Handbook/DAG design còn mô tả cũ — ngoài phạm vi, ghi rõ ở Task 8.
- Rollback: mọi thay đổi nằm trên `feature/anomaly-rule-base`; `main` giữ nguyên.
