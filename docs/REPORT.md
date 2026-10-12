# Airflow in PySpark Data Platforms

## Báo cáo thiết kế và kiến trúc

Bản báo cáo chi tiết bằng tiếng Việt nằm tại [Thiết kế DAG bán lẻ](DAG_DESIGN_VI.md), dựa trên đề `airflow_subject.docx`, sách *Data Pipelines with Apache Airflow*, Second Edition và code hiện tại.

| Nội dung báo cáo theo đề | Mục trong bản thiết kế |
|---|---|
| Phạm vi, mục tiêu và truy vết yêu cầu | 1–2 |
| DAG, task, operator, sensor, XCom, connection, hook | 3 |
| Airflow orchestration và Spark compute; anti-pattern xử lý nặng | 4 |
| Daily interval, logical date, lịch sử và landing | 5 |
| Sơ đồ DAG, operator/sensor, retry, deadline/SLA và failure behavior | 6–7, 12 |
| Schema, cleaning, Parquet year/month | 8 |
| Công thức RFM, Window scoring, KMeans, high-value và churn proxy | 9 |
| Tổng đơn và ngưỡng thống kê; phân biệt row/order audit | 10 |
| Idempotency, atomicity, versioning và recovery | 11 |
| Môi trường Docker, pool, tài nguyên và secrets | 13 |
| Khung DAG và kế hoạch triển khai | 14, 16 |
| Kiểm thử và tiêu chí nghiệm thu | 15 |
| Sản phẩm nộp và live demo | 17 |
| Rubric mới 25/25/25/15/10, tiêu chí và bằng chứng đủ 100 điểm | 18 |
| Chương/trang sách và tài liệu bổ trợ | 19 |
| Giới hạn bằng chứng hiện có | 20 |

## Trạng thái kết quả

**Cập nhật 12/10/2026:** RFM và Anomaly tự kiểm tra, ghi publication marker
và phát Asset độc lập ngay tại task của mình. notify_completion giữ all_success,
đối soát hai marker rồi công bố snapshot tổng hợp. Marketing nghe RFM;
Dashboard chỉ nghe RETAIL_SNAPSHOT_ASSET, không ghép event nhánh.
Chi tiết ở [ASSET_SCHEDULING_VI.md](ASSET_SCHEDULING_VI.md). Full-data scheduler đã chạy
ngày 11/10 theo [bằng chứng](RUN_EVIDENCE_20261011.md), nhưng dùng thiết kế
phát cả ba Asset tại notify; chưa chạy lại scheduler full-data sau thay đổi này.
Các đoạn trạng thái 06/10 bên dưới là ghi nhận lịch sử.

Cập nhật 06/10/2026 đã giữ sáu task daily và chuyển KMeans sang load/transform, bổ sung DAG training ba task, model có phiên bản và bootstrap ETL-only; xem mục 6.1–6.3, 11.5 và 16.1 của [bản thiết kế](DAG_DESIGN_VI.md). 94 tests đã qua, gồm Spark lifecycle và kiểm tra metadata/DAG; Airflow import thành công cả hai DAG. Chưa chạy scheduler smoke hai DAG trên toàn dataset; bằng chứng scheduler cũ bên dưới thuộc baseline ngày 29/09.

DAG chính thức đã có sáu task thực: EmptyOperator mở đầu, FileSensor mở rộng, ba SparkSubmitOperator và PythonOperator công bố kết quả. Code bổ sung partition/manifest, audit tổng đơn và nhãn inactivity. Kết quả kiểm thử và scheduler run nằm tại [IMPLEMENTATION_VI.md](IMPLEMENTATION_VI.md).

Tài liệu triển khai ghi phiên bản image, lệnh/kết quả test, summary và giới hạn. Graph/Grid/Gantt và slide vẫn là phần cần chọn để hoàn thiện bài nộp; không trình bày tiêu chí kiểm thử dự kiến như kết quả đã đạt.

Hướng dẫn chạy chính thức gồm bootstrap landing, pool, manual trigger và recovery: [SETUP.md](SETUP.md).
