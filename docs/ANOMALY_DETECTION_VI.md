# Phát hiện bất thường: EDA, insight và rule base hai cấp

Tài liệu này gom toàn bộ phần anomaly của dự án vào một chỗ: chuẩn tham chiếu, các kết quả EDA đã có, bằng chứng mới, insight, bộ rule kèm logic, nhật ký triển khai và kết quả chạy. Mã nguồn: [pyspark_anomalies.py](../scripts/pyspark_anomalies.py). Bảng bằng chứng: [anomaly_evidence.py](../scripts/anomaly_evidence.py). Kế hoạch: `.agent/plans/active/2026-10-08-anomaly-rule-base.md` (chuyển sang `archive/` khi xong).

## 1. Mục tiêu và phạm vi

- Phát hiện **ứng viên cần xem xét**, không kết luận gian lận hay lỗi.
- Tách hai cấp: **data anomaly** (chất lượng dữ liệu) và **business anomaly** (hành vi nghiệp vụ khác thường).
- Trước khi xét bất thường, phân loại mỗi dòng theo **loại bản ghi** và mỗi hóa đơn theo **kênh**, vì dòng kiểm kho, phí hay bút toán không thể so với dòng bán hàng.
- Khách định danh (chủ yếu sỉ) được so với **lịch sử của chính họ**. Khách lẻ không có CustomerID nên chỉ so được theo sản phẩm trong kênh lẻ.
- Đánh giá hồi cứu: chỉ dùng dữ liệu trước `run_date`. Baseline tính trên cùng tập lịch sử (in-sample), không phải chấm điểm giao dịch mới theo thời gian thực.
- Giữ nguyên mọi dòng, không sửa, không điền CustomerID.

## 2. Chuẩn tham chiếu

| Chuẩn | Dùng cho | Nội dung áp dụng |
|---|---|---|
| ISO/IEC 25012:2008 — Data Quality Model | Cấp 1 | Các đặc tính chất lượng dữ liệu cố hữu: accuracy, completeness, consistency, credibility, currentness. Mỗi data rule gắn với một đặc tính. |
| DAMA-DMBOK — sáu chiều cốt lõi | Cấp 1 | Accuracy, completeness, consistency, timeliness, uniqueness, validity. |
| Chandola, Banerjee, Kumar (2009), *Anomaly Detection: A Survey*, ACM Computing Surveys 41(3) | Cấp 2 | Ba loại: point (một bản ghi lạ so với toàn bộ), contextual (lạ trong một ngữ cảnh, ví dụ một khách cụ thể), collective (một nhóm bản ghi cùng lạ). |
| Iglewicz & Hoaglin (1993), qua NIST e-Handbook §1.3.5.17 | Ngưỡng thống kê | Modified z-score `0.6745·(x − median)/MAD`; \|z\| > 3.5 là ứng viên ngoại lai. Bền với ngoại lai hơn mean ± 3σ. |
| ISA 520 — Analytical Procedures | Ngưỡng trọng yếu | Lập kỳ vọng, đặt trước mức chênh lệch chấp nhận được không cần điều tra (threshold), rồi mới so thực tế. |
| Mô tả bộ UCI Online Retail (Chen, Sain, Guo 2012) | Ngữ cảnh | Doanh nghiệp *non-store online retail* tại Anh, chủ yếu bán quà tặng; nhiều khách là nhà bán sỉ. Không có cửa hàng vật lý. |

Nguồn:
[ISO/IEC 25012 (arc42)](https://quality.arc42.org/standards/iso-iec-25012) ·
[DAMA dimensions (IBM)](https://www.ibm.com/docs/en/DSXDOC/wsj/quality/dq-dimensions.html) ·
[Chandola et al.](https://www-users.cse.umn.edu/~kumar001/papers/anomaly-survey.php) ·
[NIST modified z-score](https://itl.nist.gov/div898/handbook/eda/section3/eda35h.htm) ·
[ISA (NZ) 520](https://standards.xrb.govt.nz/standards-navigator/isa-nz-520/) ·
[UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)

## 3. Tóm tắt EDA anomaly đã có

Nguồn: [EDA_Anomalies_Guide_VI.md](../notebooks/EDA_Anomalies_Guide_VI.md), [EDA_Online_Retail_Guide.md](../notebooks/EDA_Online_Retail_Guide.md).

| Phát hiện | Số liệu |
|---|---|
| Dòng gốc / sau loại trùng toàn bộ cột | 541,909 / 536,641 (5,268 dòng trùng) |
| Dòng thiếu CustomerID (sau loại trùng) | 135,037 (25.16%), thuộc 3,710 hóa đơn |
| Hóa đơn lẫn có/thiếu ID hoặc nhiều ID | 0 — thiếu ID xảy ra ở cấp cả hóa đơn |
| Tỷ lệ dòng hủy: thiếu ID / có ID | 0.28% / 2.21% |
| Số lượng âm không phải hủy | 1,336 dòng, đều giá 0 và thiếu ID (ghi chú kho: check, lost, smashed…) |
| Đơn giá âm | 2 dòng `Adjust bad debt`, −11,062.06 GBP |
| Giá 0 trong hóa đơn có dòng trả tiền | 33 (có ID) và 362 (thiếu ID) |

## 4. Bằng chứng mới (E1–E8)

Tái tạo:

```bash
docker compose run --rm --no-deps airflow-scheduler python /opt/airflow/scripts/anomaly_evidence.py --input /opt/airflow/data/raw/data.csv
```

Tất cả bảng tính trên 536,641 dòng sau loại trùng. Kênh và loại bản ghi theo định nghĩa ở mục 6.1.

### E1. Dòng DOT theo có/thiếu CustomerID

| Có ID | Hóa đơn | Dòng | Giá trị (GBP) | Giá trung vị |
|---|---:|---:|---:|---:|
| Không | 694 | 694 | 194,339.12 | 183.10 |
| Có | 16 | 16 | 11,906.36 | 715.90 |

### E2. Quy mô theo kênh

| Kênh | Hóa đơn | Dòng | Giá trị dòng dương (GBP) | % dòng |
|---|---:|---:|---:|---:|
| identified | 22,190 | 401,604 | 8,887,208.89 | 74.84 |
| retail_web | 694 | 120,114 | 1,222,279.69 | 22.38 |
| retail_other | 917 | 12,819 | 521,560.16 | 2.39 |
| internal | 2,099 | 2,104 | 11,062.06 | 0.39 |

### E3. Hành vi mua của dòng sale theo kênh

| Kênh | Dòng | Mode Qty | % Qty = 1 | Qty p25 / p50 / p75 / p95 | Giá TB | Giá p25 / p50 / p75 |
|---|---:|---:|---:|---|---:|---|
| identified | 391,057 | 1 | 17.61 | 2 / 6 / 12 / 40 | 2.87 | 1.25 / 1.95 / 3.75 |
| retail_web | 119,379 | 1 | 58.50 | 1 / 1 / 2 / 8 | 4.46 | 1.63 / 3.29 / 5.09 |
| retail_other | 12,015 | 1 | 32.43 | 1 / 3 / 8 / 39 | 4.53 | 2.08 / 3.29 / 5.79 |

| Thiếu ID? | Giá TB, mọi dòng dương | Giá TB, chỉ dòng sale |
|---|---:|---:|
| Có (thiếu ID) | 6.29 | 4.47 |
| Không (có ID) | 3.13 | 2.87 |

Cùng StockCode (1,297 mã có ≥ 30 dòng sale ở cả hai nhóm): giá trung vị retail / identified có trung vị **1.97**, p25–p75 là 1.95–2.04; **91.3%** mã có giá lẻ cao hơn.

### E4. Loại bản ghi × kênh (số dòng)

| record_type | identified | internal | retail_other | retail_web | Tổng |
|---|---:|---:|---:|---:|---:|
| sale | 391,057 | 0 | 12,015 | 119,379 | 522,451 |
| cancellation | 8,872 | 0 | 376 | 3 | 9,251 |
| fee_service | 1,642 | 5 | 74 | 720 | 2,441 |
| inventory_adjustment | 0 | 2,096 | 0 | 5 | 2,101 |
| zero_price_line | 33 | 0 | 354 | 7 | 394 |
| accounting_adjustment | 0 | 3 | 0 | 0 | 3 |
| Tổng | 401,604 | 2,104 | 12,819 | 120,114 | 536,641 |

### E5. Độ sâu lịch sử khách định danh

| Khách | Hóa đơn/khách p25 / p50 / p75 / p95 | % khách ≥ 5 hóa đơn |
|---:|---|---:|
| 4,334 | 1 / 2 / 5 / 13 | 25.4 |

Cấp baseline dùng được cho rule số lượng (% dòng sale):

| Cấp | identified | retail |
|---|---:|---:|
| customer_stock (≥ 5 dòng cùng khách và mã) | 14.00 | – |
| customer (≥ 20 dòng của khách) | 82.86 | – |
| channel_stock (≥ 30 dòng của mã trong nhóm) | 2.99 | 84.43 |
| Không đủ lịch sử | 0.15 | 15.57 |

### E6. Dòng kiểm kho

Giá trị ước tính = |Quantity| × giá bán trung vị của mã đó.

| Dòng | Số lượng âm | Không ước được | p50 / p90 / p99 (GBP) | ≥ 100 GBP | ≥ 1,000 GBP | Tổng ước tính (GBP) |
|---:|---:|---:|---|---:|---:|---:|
| 2,101 | 1,336 | 139 | 46.20 / 435.00 / 3,748.33 | 664 | 70 | 469,433.72 |

### E7. Giá cố định theo khách × mã

7,445 cặp khách × mã có ≥ 5 dòng sale; **97.8%** có MAD của log giá bằng 0, tức mỗi khách gần như luôn mua một mã với đúng một giá.

### E8. Hủy vượt mua theo khách × mã

| Cặp có hủy | Cặp hủy > mua | Trong đó không có lần mua nào | Dòng hủy liên quan | Giá trị (GBP) |
|---:|---:|---:|---:|---:|
| 7,792 | 967 | 891 | 1,173 | 156,752.38 |

Kiểm tra thêm 1,061 dòng hủy không có lần mua tương ứng:
- 34.4% là mã phí/đặc biệt (Manual 175, POSTAGE 97, Discount 77, CRUK Commission 16), tức hoàn phí chứ không phải trả hàng.
- Trong 12/2010, 33% dòng hủy không có lần mua tương ứng (227/681); tháng 1–2/2011 khoảng 17–20%, từ tháng 3 trở đi khoảng 6–13%: có hiệu ứng đầu kỳ (hàng mua trước 01/12/2010 không có trong dữ liệu), nhưng hiện tượng vẫn kéo dài cả năm.

### E9. Cấu trúc hóa đơn DOT

| Kênh | Dòng/hóa đơn p25 / p50 / p75 / p90 |
|---|---|
| retail_web | 48 / 149 / 224 / 411 |
| retail_other | 1 / 2 / 10 / 56 |
| identified | 3 / 12 / 24 / 43 |

Hóa đơn web: giá trị trung vị 1,338 GBP, 1–6 hóa đơn/ngày (trung vị 2) trong 225 ngày, toàn bộ ở United Kingdom, gần như không lặp mã hàng trong cùng hóa đơn (0.12%).

## 5. Insight

1. **DOT gắn gần như hoàn toàn với hóa đơn thiếu ID**: 694/710 hóa đơn DOT (97.7%). Kênh `retail_web` chiếm 22.4% số dòng và 1.22 triệu GBP, là phần lớn nhất của nhóm thiếu ID.
2. **Giá lẻ gần gấp đôi giá sỉ trên cùng sản phẩm** (trung vị 1.97 lần, 91% mã). So giá trung bình thô thì nhóm thiếu ID cao gấp 2.0 lần, nhưng một phần do phí: chỉ tính dòng sale thì còn 1.56 lần. Vì vậy baseline giá phải tách theo nhóm kênh.
3. **Mode = 1 không phải đặc trưng riêng của nhóm thiếu ID**: cả ba kênh đều có mode 1. Khác biệt nằm ở tỷ trọng (58.5% dòng web có Qty = 1, so với 17.6% ở identified) và trung vị (1 so với 6).
4. **Một hóa đơn DOT có thể không phải một khách**: trung vị 149 dòng và 1,338 GBP mỗi hóa đơn, vài hóa đơn mỗi ngày. Có thể đây là bút toán gộp đơn web, hoặc giỏ hàng rất lớn; dữ liệu không đủ để chốt. Hệ quả: so sánh tổng hóa đơn trong kênh lẻ là so các "bút toán hóa đơn", không phải so từng người mua.
5. **`retail_other` là kênh chưa xác định**: 917 hóa đơn, trung vị 2 dòng, giá giống kênh lẻ. Công ty không có cửa hàng, nên đây có thể là đơn điện thoại/email, đơn qua marketplace hoặc nhập tay; không gắn nhãn "tại cửa hàng".
6. **Kiểm kho là sổ cần đối soát, không phải nhiễu**: 2,101 dòng, ước tính 469k GBP, 70 dòng ≥ 1,000 GBP. Dòng kiểm kho dồn ở kênh `internal`.
7. **Lịch sử khách mỏng ở cấp hóa đơn, dày ở cấp dòng**: trung vị chỉ 2 hóa đơn/khách (25.4% khách có ≥ 5), nhưng 96.9% dòng sale của khách định danh có baseline cấp khách. Baseline hóa đơn theo khách cần lùi về baseline kênh cho đa số khách.
8. **Giá theo khách gần như cố định** (97.8% cặp có MAD = 0): z-score không dùng được một mình vì mọi chênh lệch đều cho z vô hạn. Ngưỡng trọng yếu (lệch ≥ 30%) là điều kiện quyết định.
9. **Hủy vượt mua cần lọc trước khi kết luận**: loại mã phí và tính đến đầu kỳ dữ liệu, nếu không rule sẽ báo nhầm hàng trăm dòng hoàn phí.

## 6. Rule base

### 6.1. Tầng 0 — phân loại (không phải anomaly)

`record_type`, theo thứ tự ưu tiên, loại trừ nhau:

| # | record_type | Điều kiện (sau trim/upper) |
|---|---|---|
| 1 | `accounting_adjustment` | InvoiceNo bắt đầu `A`, hoặc StockCode = `B`, hoặc Description = `ADJUST BAD DEBT` |
| 2 | `cancellation` | InvoiceNo bắt đầu `C` |
| 3 | `fee_service` | StockCode/Description thuộc danh sách phí RFM, hoặc StockCode chỉ gồm chữ cái và khoảng trắng (POST, DOT, M, BANK CHARGES, AMAZONFEE, CRUK, D, S…) |
| 4 | `inventory_adjustment` | UnitPrice = 0, thiếu CustomerID, hóa đơn không có dòng UnitPrice > 0 |
| 5 | `sale` | Quantity > 0, UnitPrice > 0, line_value hữu hạn > 0 |
| 6 | `zero_price_line` | UnitPrice = 0, Quantity > 0 (còn lại sau #4) |
| 7 | `unclassified` | Còn lại |

`channel`, cấp hóa đơn:

| channel | Điều kiện |
|---|---|
| `identified` | Hóa đơn có ≥ 1 dòng có CustomerID |
| `retail_web` | Không có ID, có dòng StockCode `DOT` |
| `retail_other` | Không có ID, không DOT, có dòng UnitPrice > 0 không phải `accounting_adjustment` |
| `internal` | Không có ID, không có dòng nào thỏa trên |
| `unknown` | Thiếu InvoiceNo |

Baseline lẻ gộp `retail_web` và `retail_other` thành nhóm `retail`; baseline định danh dùng nhóm `identified`.

### 6.2. Tầng 1 — Data anomaly

Thiếu CustomerID **không** phải data anomaly; đó là thuộc tính kênh.

| Rule | Chiều chất lượng | Flag khi | Severity |
|---|---|---|---|
| `missing_required_field` | Completeness | Thiếu InvoiceNo/StockCode/InvoiceDate/Quantity/UnitPrice/Country | high |
| `missing_description` | Completeness | Description thiếu | low |
| `invalid_numeric_value` | Validity | Quantity null hoặc UnitPrice/line_value không hữu hạn | high |
| `cancel_sign_conflict` | Consistency | Hóa đơn hủy nhưng Quantity ≥ 0 | medium |
| `negative_price_outside_adjustment` | Validity | UnitPrice < 0 mà không phải bút toán | high |
| `invoice_multiple_customers` | Consistency | Hóa đơn có ≥ 2 CustomerID | medium |
| `invoice_inconsistent_header` | Consistency | Hóa đơn có > 1 Country hoặc > 1 ngày | medium |

Dòng trùng (Uniqueness, 5,268 dòng) đã bị loại ở bước clean nên không có rule dòng.

### 6.3. Tầng 2 — Business anomaly

Ký hiệu: `x = ln(giá trị)`; `med`, `MAD` là trung vị và độ lệch tuyệt đối trung vị của baseline trên thang log; `z = 0.6745·(x − med)/MAD`; `fold = exp(x − med)` (bội số so với mức thường). Khi MAD = 0, `z` để trống và điều kiện thống kê coi là thỏa nếu `x ≠ med` theo đúng chiều; ngưỡng trọng yếu quyết định.

Một dòng chỉ bị flag khi thỏa **cả** điều kiện thống kê **và** ngưỡng trọng yếu (đặt trước khi xem kết quả, theo tinh thần ISA 520).

| Rule | Loại | Áp dụng | Baseline (cấp đầu tiên đủ mẫu) | Flag khi | value_at_risk |
|---|---|---|---|---|---|
| `quantity_deviation` | contextual | sale | identified: khách × mã (≥ 5) → khách (≥ 20, theo tỷ lệ Qty / trung vị Qty của mã) → kênh × mã (≥ 30); retail: kênh × mã | z > 3.5 và fold ≥ 3 | (Qty − Qty kỳ vọng) × UnitPrice |
| `price_deviation` | contextual | sale | identified: khách × mã (≥ 5) → kênh × mã (≥ 30); retail: kênh × mã | \|z\| > 3.5 và \|fold − 1\| ≥ 30% | \|giá − giá kỳ vọng\| × Qty |
| `zero_price_sale_line` | point | dòng giá 0 trong hóa đơn có dòng trả tiền | — | luôn | Qty × giá bán trung vị của mã |
| `all_zero_price_invoice` | collective | dòng giá 0 của hóa đơn có ID mà mọi dòng giá 0 | — | luôn | như trên |
| `inventory_adjustment` | point | dòng kiểm kho | — | luôn | \|Qty\| × giá bán trung vị của mã |
| `manual_accounting_entry` | point | bút toán | — | luôn, severity high | \|line_value\| |
| `unmatched_return` | collective | dòng hủy có ID, không phải mã phí/đặc biệt, sau 30 ngày đầu của lịch sử | tổng theo khách × mã | \|Σ Qty hủy\| > Σ Qty mua | \|line_value\| |

Cấp hóa đơn: `order_value_deviation` so tổng hóa đơn đủ điều kiện với baseline khách (≥ 5 hóa đơn) → kênh (≥ 30 hóa đơn trong nhóm identified hoặc retail); flag khi z > 3.5 và fold ≥ 3.

`unmatched_return` loại hai trường hợp theo bằng chứng E8: hủy mã phí/đặc biệt (Manual, POSTAGE, Discount…) là hoàn phí chứ không phải trả hàng (`service_or_special_code`); hủy trong 30 ngày đầu của lịch sử có thể ứng với hàng mua trước khi dữ liệu bắt đầu (`history_window_start`).

Severity theo value_at_risk: high ≥ 1,000 GBP, medium ≥ 100 GBP, còn lại low (không ước được giá trị thì low).

## 7. Tham khảo đã cân nhắc

[Tracy-yyq/ecommerce-analytics](https://github.com/Tracy-yyq/ecommerce-analytics) làm anomaly cấp khách bằng Isolation Forest với `contamination = 0.05` trên đặc trưng tổng hợp mỗi khách, dữ liệu là sự kiện xem/giỏ/mua tháng 10/2019. Không áp dụng vì:
- Tỷ lệ bất thường bị ép cố định 5%, luôn có đúng 5% khách bị flag dù dữ liệu sạch hay bẩn.
- Mỗi flag không kèm lý do hay rule cụ thể, khó đối soát nghiệp vụ.
- Họ so khách với khách; ở đây cần so mỗi khách với lịch sử của chính họ (contextual).

## 8. Nhật ký triển khai

| Task | Nội dung | Kết quả | Kiểm chứng |
|---|---|---|---|
| 1 | Script bằng chứng E1–E8, khung tài liệu | Bảng mục 4; hằng số giữ nguyên; hai điều chỉnh cho `unmatched_return` (xem mục 6.3, ghi ở Task 5) | `pytest tests/test_anomaly_evidence.py` → 1 passed; script chạy exit 0 trên 536,641 dòng |
| 2 | `add_invoice_summary`, `add_record_context` trong Spark | Mỗi dòng có `record_type`, `channel`, `has_dotcom_postage`; quy tắc khớp bảng 6.1 | `test_record_type_priority_and_channels` (14 dòng, đủ 7 loại và 5 kênh) → pass; suite anomaly 8 passed |
| 3 | Tầng 1: `DATA_RULES`, struct check mới (tier, context_level, robust_z, fold_change, value_at_risk, severity) | 7 data rule; thiếu CustomerID không bị flag; bút toán nợ xấu không bị tính giá âm | `test_data_rules_dimensions_and_severity` → pass; suite 9 passed |
| 4 | `robust_baseline` (median/MAD chính xác trên log), `quantity_deviation`, `price_deviation` | Cấp baseline khách × mã → khách → kênh × mã; MAD = 0 do ngưỡng trọng yếu quyết định; identified không bao giờ dùng baseline lẻ | `test_quantity_hierarchy_levels`, `test_price_deviation_zero_mad_and_direction`, `test_identified_never_uses_retail_baseline` → pass; suite 12 passed |
| 5 | `BUSINESS_RULES`, `add_operational_checks`: giá 0, kiểm kho, bút toán, hủy vượt mua | value_at_risk ước tính bằng giá bán trung vị của mã; `unmatched_return` loại mã phí và 30 ngày đầu | `test_operational_rules_value_at_risk`, `test_unmatched_return_collective` → pass; suite 14 passed |

## 9. Kết quả chạy đầy đủ

Điền sau khi triển khai xong (Task 8).

## 10. Giới hạn

- Không có nhãn đúng/sai, nên không đo được precision/recall; flag là ứng viên cần xem xét.
- Kênh `retail_web` dựa trên sự có mặt của DOT; một hóa đơn web không có dòng DOT sẽ rơi vào `retail_other`.
- Baseline in-sample: dòng được đánh giá cũng nằm trong baseline; median/MAD hạn chế ảnh hưởng nhưng không loại bỏ hoàn toàn.
- Hiệu ứng đầu kỳ: giao dịch trước 01/12/2010 không có trong dữ liệu.
