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

DAG chính thức đã có sáu task thực: EmptyOperator mở đầu, FileSensor mở rộng, ba SparkSubmitOperator và PythonOperator công bố kết quả. Code bổ sung partition/manifest, audit tổng đơn và nhãn inactivity. Kết quả kiểm thử và scheduler run nằm tại [IMPLEMENTATION_VI.md](IMPLEMENTATION_VI.md).

Tài liệu triển khai ghi phiên bản image, lệnh/kết quả test, summary và giới hạn. Graph/Grid/Gantt và slide vẫn là phần cần chọn để hoàn thiện bài nộp; không trình bày tiêu chí kiểm thử dự kiến như kết quả đã đạt.

Hướng dẫn chạy chính thức gồm bootstrap landing, pool, manual trigger và recovery: [SETUP.md](SETUP.md).
