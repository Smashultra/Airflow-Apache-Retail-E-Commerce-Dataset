# Chạy đầy đủ pipeline ngày 11/10/2026

Đã chạy qua scheduler thật: bootstrap ETL → training → daily → hai consumer.
**4 DAG, 11 task thành công ngay lần đầu**, không cần retry.

| Thành phần | Run / kết quả |
|---|---|
| Bootstrap ETL | `bootstrap_full_stack_20261011`; cutoff `2011-12-01` |
| Training | `training_full_stack_20261011`; 3 task success |
| Daily | `daily_full_stack_20261011`; 6 task success |
| Marketing | `asset_triggered`; nhận event ID 2; success |
| Executive | `asset_triggered`; nhận event IDs 2 và 3; success |

Training chạy 11:47:59–11:49:41, daily 11:50:58–11:59:10,
hai consumer hoàn tất lúc 11:59:12 (UTC+07:00).
Ngày nghiệp vụ là `2011-12-09`, cutoff loại trừ là `2011-12-10`.
Model: `0141654d68d044d49d6b1d1fddfef689`, k=2, training cutoff `2011-12-01`.

| Chỉ số | Giá trị |
|---|---:|
| Dòng nguồn daily | 541.909 |
| Dòng trùng được loại | 5.268 |
| Lỗi parse | 0 |
| Giao dịch sạch cho RFM | 391.057 |
| Khách hàng RFM | 4.334 |
| Dòng audit | 536.641 |
| Dòng audit bị gắn cờ | 46.341 |
| Tổng đơn audit | 25.900 |
| Đơn bị gắn cờ | 54 |
| Đơn không bị gắn cờ | 19.852 |
| Đơn không đủ điều kiện đánh giá | 5.994 |

Kiểm tra đọc lại bằng PyArrow và metadata, không khởi động thêm Spark:
`python /opt/airflow/logs/full_stack_20261011/verify.py` → exit 0,
`verified: true`. Số dòng Parquet khớp manifest; `_SUCCESS` đầy đủ;
context/ETL/source/model hashes hợp lệ; CSV gốc không đổi; cột model_version
đúng model đã công bố; mẫu audit có 14 check. Cả ba Asset events do
`notify_completion` phát, cùng run_key và completion manifest; hai consumer
được scheduler kích hoạt từ đúng các event đó.

Bằng chứng cục bộ (không đưa vào Git): `logs/full_stack_20261011/verification.json`,
`published.json`, `runs_latest.json`, `transitions.jsonl`, `bootstrap.log`,
và task logs của các run trên. Con trỏ dữ liệu đã công bố:
`data/manifests/published/2011-12-10.json`.

Pool `retail_spark` giữ 1 slot. Audit gặp giới hạn codegen Java 64 KB và
spill cache xuống đĩa; Spark fallback rồi hoàn tất thành công. Đây là điểm
cần theo dõi hiệu năng, không phải bằng chứng không có cảnh báo tài nguyên.
Consumer hiện chỉ kiểm tra metadata và ghi log demo, chưa gửi voucher hay
cập nhật dashboard ngoài. DAG vẫn dùng snapshot ETL đầy đủ, chưa chuyển sang
CLI incremental store. Chưa cài OpenLineage; phần này được hoãn theo yêu cầu.

Sau demo: c? 4 DAG ?? paused; kh?ng c?n DagRun running/queued.
