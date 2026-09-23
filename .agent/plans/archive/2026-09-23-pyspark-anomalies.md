# Kế hoạch triển khai PySpark anomalies detection

- Ngày: 2026-09-23.
- Trạng thái: hoàn thành, lưu trữ sau kiểm chứng.
- Owner: Codex. Nhánh hiện tại: main.
- Nguồn yêu cầu: kế hoạch chi tiết được người dùng yêu cầu lưu và xuất bản trong cuộc trao đổi.
- Cơ sở: EDA anomalies và bản sửa đối chiếu Description theo StockCode. Notebook/hướng dẫn đang có cục bộ; kế hoạch này tự chứa đặc tả để triển khai.

## 1. Mục tiêu và giới hạn

Triển khai scripts/pyspark_anomalies.py bằng PySpark để đánh giá hồi cứu từng dòng giao dịch, giữ dữ liệu gốc, trả cờ và bằng chứng từng quy tắc. Không xóa dòng, không điền CustomerID, không kết luận gian lận, không tự sửa dữ liệu.

Bao gồm job, tests/test_pyspark_anomalies.py, hướng dẫn README, ignore kết quả phát sinh và cập nhật .agent. Không sửa cleaning/RFM, không nối Airflow, không triển khai ML/điểm rủi ro hoặc ghép hoàn/hủy tự động.

Quy ước đã được người dùng chọn: dùng lịch sử **trước run-date**, đồng nhất RFM. Ví dụ 2011-12-10 bao gồm giao dịch đến hết 09/12. Dòng thiếu ngày vẫn giữ cho các quy tắc nghiệp vụ phù hợp nhưng không tham gia xây dựng/đánh giá IQR hoặc xây dựng tên tham chiếu. Ngưỡng dùng cùng tập lịch sử được đánh giá; đây là hồi cứu, không phải chấm giao dịch mới chỉ bằng quá khứ trước từng dòng.

## 2. CLI và đầu ra

| Tham số | Quy định |
|---|---|
| --input | Bắt buộc: thư mục Parquet đầu vào |
| --output | Bắt buộc: thư mục gốc kết quả |
| --run-date | Bắt buộc: ngày hợp lệ YYYY-MM-DD, cutoff exclusive |
| --iqr-multiplier | Mặc định 3.0, hữu hạn và > 0 |
| --min-samples | Mặc định 30, số nguyên >= 2 |

```bash
spark-submit /opt/airflow/scripts/pyspark_anomalies.py \
  --input /opt/airflow/data/audit/anomalies.parquet \
  --output /opt/airflow/data/audit/anomaly_results \
  --run-date 2011-12-10
```

Ghi vào output/run_date=YYYY-MM-DD/. Chỉ overwrite phân vùng ngày được yêu cầu, giữ ngày khác. Chuẩn hóa đường dẫn; từ chối input/output trùng hoặc lồng nhau trước khi ghi. Kiểm tra kết quả trước ghi. Không cam kết atomic write trên mọi filesystem; lỗi phải được báo và có thể chạy lại cùng ngày.

Giữ tám cột gốc, thêm:

| Cột | Kiểu / ý nghĩa |
|---|---|
| run_date | Date |
| reference_mode | String: retrospective_before_run_date |
| iqr_multiplier | Double |
| min_samples | Integer |
| line_value | Double; null khi không hữu hạn |
| data_quality_flags | Array<string> |
| context_flags | Array<string> |
| check_results | Array<struct> |
| anomaly_flags | Array<string>, tên quy tắc flagged |
| assessment_status | flagged / not_flagged / not_assessable |

Mỗi check_results có các trường:

```text
rule: string
status: flagged | not_flagged | not_applied
reason: string hoặc null
observed_value: double hoặc null
reference_count: long hoặc null
q1: double hoặc null
q3: double hoặc null
upper_bound: double hoặc null
```

Thứ tự quy tắc/cờ cố định. Không duy trì thêm các danh sách checks_applied/checks_not_applied trùng thông tin với check_results.

Trạng thái tổng được tính **sau đánh giá**: có bất kỳ flagged -> flagged; không có cờ nhưng có phép đã áp dụng -> not_flagged; không phép nào áp dụng -> not_assessable. Không áp dụng được IQR không đồng nghĩa không đánh giá được giao dịch.

## 3. Chi tiết xử lý

### 3.1. Hợp đồng đầu vào và thời gian

Đọc Parquet bằng Spark; kiểm tra tám cột và kiểu schema giao dịch hiện tại. Thiếu cột/sai kiểu cấu trúc thì dừng; null từng dòng thì giữ và gắn cờ. Chọn InvoiceDate < run_date cộng dòng thiếu ngày. Log số đầu vào, số ngoài cutoff và số thiếu ngày. Không dropDuplicates lần nữa, giữ nguyên số lần xuất hiện.

SparkSession dùng UTC và ANSI như RFM; không suy diễn thêm timezone nguồn.

### 3.2. Cột tính toán và chất lượng dữ liệu

Tạo cột chuẩn hóa riêng cho InvoiceNo, StockCode, CustomerID bằng trim/upper; Description thêm thu gọn khoảng trắng. Null/rỗng/toàn khoảng trắng là thiếu. Không sửa cột gốc. Tính Quantity * UnitPrice thành line_value nếu hữu hạn; bỏ cột nội bộ không cần thiết khi tạo đầu ra.

Cờ chất lượng:

```text
missing_customer_id, missing_invoice_no, missing_stock_code,
missing_description, missing_country, missing_invoice_date,
invalid_quantity, invalid_unit_price, invalid_line_value
```

Số âm không mặc định là invalid. Các cờ chất lượng không tự tạo anomaly_flags.

### 3.3. Ngữ cảnh nghiệp vụ và tên tham chiếu

Tạo cancelled_invoice, service_line, special_stock_code, zero_negative, zero_positive, sale_shape. Tái sử dụng danh sách mã/mô tả hiện có từ cleaning, không tạo bản sao độc lập.

Tên tham chiếu lấy từ dòng có ngày hợp lệ, có InvoiceNo/StockCode, không C, quantity/price/line_value dương hữu hạn, không phí hoặc mã chữ đặc biệt. Mỗi cặp StockCode–Description chuẩn hóa cần >= 2 InvoiceNo chuẩn hóa khác nhau; giữ mọi tên đạt điều kiện.

Dùng biểu thức từ khóa đầy đủ đã sửa trong notebook (STOCK không bắt STOCKING; hỗ trợ các biến thể rõ như DAMAGED). Trùng tên cùng mã -> keyword_product_name; có từ khóa nhưng khác tên của mã có tham chiếu -> stock_words/damage_words/loss_words/coding_words; không đủ tham chiếu -> keyword_unresolved.

Chỉ lưu vào context_flags, chưa biến từ khóa thành cảnh báo hoặc tự loại khỏi IQR. Bảng tham chiếu phải duy nhất theo khóa join để tránh fanout.

### 3.4. Sáu quy tắc nghiệp vụ

Tổng hợp theo InvoiceNo chuẩn hóa trong phạm vi; không gom InvoiceNo thiếu thành hóa đơn giả.

| Rule | Điều kiện áp dụng | Flagged khi |
|---|---|---|
| cancel_nonnegative_quantity | Có InvoiceNo, quantity hợp lệ | C và quantity >= 0 |
| negative_quantity_non_cancel | Có InvoiceNo, quantity hợp lệ | Không C và quantity < 0 |
| negative_unit_price | Price hữu hạn | Price < 0 |
| zero_price_in_priced_invoice | Có InvoiceNo, giá dòng hợp lệ; đã thấy giá dương hoặc mọi giá trong hóa đơn hợp lệ | Giá dòng = 0 và cùng hóa đơn có giá dương |
| all_zero_price_invoice | Có InvoiceNo và mọi giá trong hóa đơn hữu hạn | Mọi giá = 0 |
| multiple_customer_ids | Có InvoiceNo; đã thấy nhiều ID hoặc mọi dòng có ID | >= 2 ID chuẩn hóa khác nhau |

Giá thiếu trong hóa đơn và chưa thấy giá dương -> không đánh giá phép zero_price_in_priced_invoice. Có giá thiếu -> không đánh giá all_zero_price_invoice. Thiếu ID và chưa thấy >=2 ID khác nhau -> không đánh giá multiple_customer_ids; nếu đã thấy >=2 thì vẫn flag. Ghi not_applied và lý do, không biến thiếu thông tin thành kết luận âm tính.

Giá dương chỉ là có ghi giá, không phải bằng chứng đã thanh toán. Kết quả ở cấp dòng; log riêng số dòng và số InvoiceNo duy nhất theo quy tắc.

### 3.5. Ba quy tắc IQR

Dòng đủ điều kiện: có ngày hợp lệ trước cutoff, InvoiceNo/StockCode đầy đủ, không C, quantity/price/line_value dương hữu hạn, không phí/dịch vụ hoặc StockCode chỉ gồm chữ và khoảng trắng. Không yêu cầu CustomerID.

Theo StockCode chuẩn hóa, đánh giá Quantity -> high_quantity; UnitPrice -> high_unit_price; line_value -> high_line_value.

Dùng percentile liên tục **chính xác** của Spark cho Q1/Q3, không percentile_approx; kiểm thử cách nội suy bằng fixture biết trước. Ngưỡng:

```text
IQR = Q3 - Q1
upper_bound = Q3 + iqr_multiplier * IQR
flagged iff observed_value > upper_bound
```

n là số dòng đủ điều kiện. Chỉ áp dụng khi n >= min_samples, IQR > 0 và ngưỡng hữu hạn. Không ngưỡng dưới, không fallback toàn cục, không thêm phương pháp thay thế zero-IQR trong v1. Mức 3 là cấu hình khởi đầu, chưa phải ngưỡng đã xác thực bằng nhãn.

Lý do đầu tiên ngăn áp dụng theo thứ tự:

```text
missing_invoice_date
missing_invoice_no
missing_stock_code
invalid_numeric_value
outside_sales_scope
service_or_special_code
insufficient_product_samples
zero_iqr
nonfinite_threshold
```

Giữ đầy đủ data_quality_flags song song. Lưu observed_value, n, Q1, Q3 và upper_bound để giải thích cờ.

### 3.6. Tổng hợp kết quả và phạm vi đã kiểm tra

Mỗi dòng có đúng chín check_results. Tạo anomaly_flags từ flagged; tính assessment_status sau đó. Log trạng thái, số dòng/hóa đơn theo cờ, not_applied theo lý do. Không cộng số cờ để suy ra số giao dịch duy nhất. Không gán severity/điểm rủi ro tùy ý.

## 4. Tổ chức code

Hàm theo luồng:

```text
parse_args
validate_input_contract
select_history
add_base_columns
add_description_context
add_invoice_checks
add_product_iqr_checks
build_output
validate_output
write_results
main
```

Dùng Spark DataFrame/functions; không pandas/PyArrow/Python UDF trong job. Không class, rule engine, factory hoặc YAML. Chỉ helper nhỏ cho biểu thức lặp như hữu hạn/tạo check result. Type hint, docstring ngắn; tên biến rõ (invoice_summary, product_reference, quantity_upper_bound). Comment nêu lý do nghiệp vụ/giới hạn.

Gom thống kê ba biến trong một bảng theo sản phẩm, join một lần. Không collect giao dịch về driver; chỉ số tổng hợp nhỏ cho log. Cache kết quả cuối cho kiểm tra/log/ghi và unpersist trong finally; luôn stop Spark. Import hằng số phải chạy cả spark-submit trực tiếp và pytest import, không sửa sys.path tùy tiện.

## 5. Kiểm thử và nghiệm thu

- [x] CLI: ngày sai, multiplier không hữu hạn/không dương, min-samples sai.
- [x] Ngày: trước/ngay/sau cutoff, null ngày; dữ liệu tương lai không vào tham chiếu hoặc output.
- [x] Data quality: thiếu ID vẫn đánh giá; null/NaN/infinity đúng cờ; số âm không mặc định lỗi kiểu.
- [x] Description: 85084/STOCKING; CHECK/MOULD là tên cùng mã; nhiều tên; mã khác; thiếu tên; hai dòng một hóa đơn không đạt tham chiếu.
- [x] Mỗi business rule có positive/negative; hóa đơn [0,null], [0,positive,null], toàn 0; nhiều ID; thiếu một phần/toàn bộ ID; thiếu InvoiceNo.
- [x] IQR: Q1/Q3 nội suy biết trước; bằng/vượt ngưỡng; đủ/thiếu 30 mẫu; IQR=0; thiếu ID; phí/hủy bị loại khỏi tham chiếu.
- [x] Bảo toàn số dòng và đa tập tám cột gốc, đủ 9 check results, không fanout.
- [x] Parquet round-trip; overwrite ngày hiện tại không ảnh hưởng ngày khác; chặn đường dẫn chồng lấn.
- [x] Tập rỗng hợp lệ: ghi output rỗng đúng schema, log rõ; không tạo ngưỡng giả.

Chạy tests job mới và regression cleaning/RFM trong Docker. Nếu daemon chưa chạy, báo blocker chính xác; kiểm tra syntax không thay Spark test.

Sau tests đạt, chạy dữ liệu thật một lần với run-date=2011-12-10 vào **thư mục kiểm chứng mới**, không đụng kết quả người dùng. Đối chiếu số dòng, đa tập, cờ nghiệp vụ và IQR với notebook; giải thích khác biệt do hoàn thiện trạng thái dữ liệu thiếu. Kiểm tra SHA-256 input trước/sau. Ghi lệnh/kết quả thực tế, giới hạn hồi cứu; git diff --check và diff đúng phạm vi.

Hoàn thành khi job/test chạy thành công, output truy được bằng chứng, giữ dữ liệu nguồn, cập nhật TODO/HANDOFF và chuyển kế hoạch sang archive. Chỉ nghiệm thu hành vi kỹ thuật/khả năng đối soát, không tuyên bố độ chính xác gian lận.

## 6. Trạng thái bàn giao

Kế hoạch được xuất bản riêng theo yêu cầu người dùng. Phạm vi thực thi không bao gồm tích hợp Airflow hay tự động commit/push detector. Notebook EDA, hướng dẫn và kế hoạch EDA lưu trữ đã được commit/push riêng tại `3a0a9a6` theo yêu cầu tiếp theo của người dùng.

## 7. Kết quả triển khai

- Detector, test, README và ignore rule đã triển khai. `docker compose run --rm --no-deps airflow-scheduler python -m pytest -q -p no:cacheprovider /opt/airflow/tests/test_pyspark_clean.py /opt/airflow/tests/test_pyspark_rfm.py /opt/airflow/tests/test_pyspark_anomalies.py`: 50 passed, 1 cảnh báo pandas từ PySpark.
- Chạy dữ liệu thật bằng `spark-submit --master local[2] --driver-memory 3g`, run-date 2011-12-10: exit 0, 536.641 dòng. Đọc lại Parquet xác nhận đa tập tám cột gốc bằng input, mỗi dòng có chín check. SHA-256 thư mục input trước/sau không đổi: `cb1647b8f07eec5bd2fbb772aaff61b3739ef6efa4a2b5a034facced6dd0c55c`.
- Sáu cờ nghiệp vụ và Quantity IQR khớp notebook. UnitPrice IQR lệch +19 và line-value IQR lệch -2 dòng vì các giá trị đúng bằng ngưỡng bị ảnh hưởng bởi sai khác biểu diễn số thực ở phân vị Spark và pandas; đã xác định bốn StockCode liên quan trong thư mục kiểm chứng cục bộ.
- Output kiểm chứng mới nằm ở `data/audit/anomaly_results_verification_2026-09-23/` và bị Git bỏ qua. Chưa nối Airflow. Bộ nhớ mặc định 1 GB của Spark không đủ; cấu hình chạy đạt được ghi trong README. Kiểm tra cấu trúc host không liên quan vẫn thất bại do thiếu `.env.example`.
