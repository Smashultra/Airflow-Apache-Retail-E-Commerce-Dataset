# DAG chính thức và bằng chứng kiểm chứng

Ngày triển khai: 29/09/2026. Căn cứ: [thiết kế theo sách và rubric](DAG_DESIGN_VI.md).

## Cập nhật hai DAG — 06/10/2026

- Daily giữ sáu task; `run_rfm` chỉ nạp PipelineModel và transform. Sau tích hợp 2026-10-11, `pyspark_rfm.py` giữ CLI incremental chỉ load/score từ upstream; adapter `--pipeline-context` giữ snapshot/manifest của DAG. Hàm fit dùng chung `train_rfm_kmeans.train_model`, chỉ được gọi trong training. Xem `RFM_DAG_HANDOFF_VI.md`.
- [retail_rfm_training_dag.py](../dags/retail_rfm_training_dag.py) có ba task: validate input → Spark training → publish model. Cả hai DAG dùng chung pool retail_spark; training đọc ETL đã commit, không cần daily completion.
- [retail_models.py](../scripts/retail_models.py) quản lý model/activation hash, hiệu lực và context pinning. Registry là JSON atomic dưới lock; artifact model immutable. Retry giữ model; model tương lai, source/feature không tương thích hoặc artifact hỏng bị từ chối.
- [retail_training.py](../scripts/retail_training.py) cung cấp bootstrap ETL-only và training; lưu full scaler/KMeans, kiểm tra save/load assignment, centroid/scaler hữu hạn và occupied clusters trên tập fit. Daily có thể chỉ dùng một phần cụm.
- ETL thêm index metadata theo source/cutoff; thuật toán ETL/audit giữ nguyên. Daily vẫn dùng cumulative history và percent_rank; không tuyên bố ngưỡng Marketing cố định.
- Hướng dẫn bootstrap, trigger training trước daily và recovery nằm ở [SETUP](SETUP.md). Model đầu tiên của dữ liệu thật chưa được tạo trong lượt sửa code này; tests dùng namespace tạm.

Kiểm chứng trong image hiện có:

```text
python -m pytest -q tests/test_retail_contracts.py tests/test_retail_models.py tests/test_retail_dag.py
19 passed in 9.03s

python -m pytest -q tests/test_retail_training.py tests/test_retail_pipeline.py tests/test_pyspark_rfm.py tests/test_pyspark_clean.py tests/test_pyspark_anomalies.py
75 passed, 1 warning in 245.82s
```

Lifecycle test chạy Spark thật: bootstrap → training → artifact round-trip → activation → daily ETL/RFM/audit → completion → read-back. Khi chạy daily, test thay `Pipeline.fit` bằng hàm gây lỗi để xác nhận không refit; kiểm tra scoring một khách với k=2 vẫn hợp lệ. Tests bổ sung xác nhận activation mới không đổi run đã pin, ngày/nguồn/feature sai bị chặn, artifact hỏng và registry replace thất bại không sửa bản công bố trước.

Ruff 0.15.7 kiểm tra `dags scripts tests` đạt; Airflow `dags list-import-errors` trả `No data found`, `dags list` nhận diện hai DAG, cả hai paused. Cảnh báo Spark là upstream pandas >=3.0 compatibility. Chưa có scheduler smoke hai DAG trên full dataset, fault injection JVM hoặc đo concurrency mới; các số liệu scheduler/full-data dưới đây là bằng chứng baseline 29/09, không phải kết quả của kiến trúc mới.

## Code và lựa chọn thực tế

| Phần | File | Lý do |
|---|---|---|
| Sáu task và quan hệ `>>` | [ecommerce_etl_dag.py](../dags/ecommerce_etl_dag.py) | Airflow điều phối; ba SparkSubmitOperator chạy tính toán trong tiến trình Spark |
| Sensor, ngày nghiệp vụ, callback | [tasks.py](../dags/retail_support/tasks.py) | Manifest chưa tới thì reschedule; dữ liệu đã commit nhưng sai thì fail-fast |
| Chuẩn bị landing theo ngày | [prepare_daily_landing.py](../scripts/prepare_daily_landing.py) | Giữ CSV gốc; chuyển CP1252 sang UTF-8 có kiểm tra round-trip; ngày không bán hàng vẫn có partition rỗng hợp lệ |
| Context và manifest | [retail_contracts.py](../scripts/retail_contracts.py) | Cố định input của mỗi run; chỉ công bố các nhánh đọc cùng một ETL generation |
| Tính toán Spark | [retail_pipeline.py](../scripts/retail_pipeline.py) | Tái sử dụng thuật toán legacy, bổ sung parsing/quarantine, partition, nhãn khách và audit tổng đơn |
| Môi trường | [Compose](../docker-compose.yaml), [SETUP](SETUP.md) | Cùng volume, connection và Python path; một pool slot giới hạn JVM |

`business_date=D` là ngày dữ liệu cuối cùng được sử dụng; cutoff `C=D+1` là
mốc loại trừ. Ví dụ D=2011-12-09 dùng giao dịch trước 2011-12-10. Manual run bắt
buộc nhập D; scheduled/backfill lấy D từ data interval. Lịch bị giới hạn theo
dataset lịch sử, tránh dùng năm hiện tại để tính Recency.

XCom chỉ chuyển metadata và đường dẫn context JSON. CSV/Parquet không đi qua
metadata database. Sensor kiểm tra header/size/coverage; Spark kiểm tra SHA-256
trước khi xử lý. Dữ liệu không parse được có output `parse_errors` với giá trị
gốc và được giữ trong luồng audit với trường đã parse thành null.

ETL ghi giao dịch RFM theo `year/month`; RFM tính Recency, số hóa đơn phân biệt,
Monetary, điểm Window, KMeans, nhãn high-value và inactivity proxy. Audit giữ
chín kiểm tra trên dòng và bổ sung tổng hóa đơn: chỉ áp dụng khi có ít nhất 30
đơn hợp lệ, độ lệch chuẩn dương; ngưỡng là mean + 3×sample standard deviation,
đơn vị GBP. Một dòng không hợp lệ làm tổng đơn không đủ điều kiện; không lấy
tổng một phần để đánh giá. Thiếu CustomerID không tự loại một đơn hợp lệ.

Mỗi lần thử Spark ghi một generation mới và chỉ công bố stage manifest sau khi
ghi đủ output. Completion kiểm tra context hash, ETL hash và `_SUCCESS`, rồi
thay atomically một file JSON con trỏ. Retry không ghi đè output đã công bố;
đổi source/code/params yêu cầu DagRun mới. Đây là tính nhất quán của publication
trên filesystem local, không phải giao dịch phân tán giữa mọi file Parquet.

## Kiểm thử đã chạy

```powershell
docker compose exec -T airflow-scheduler python -m pytest -q tests/test_retail_contracts.py tests/test_retail_dag.py tests/test_retail_pipeline.py tests/test_pyspark_clean.py tests/test_pyspark_rfm.py tests/test_pyspark_anomalies.py
```

Kết quả: **87 passed**, 281,51 giây. Một cảnh báo upstream về hỗ trợ pandas 3
trong PySpark; không có test failed. Các ca gồm coverage/ngày rỗng, checksum,
path traversal, context bất biến, publication lỗi giữa chừng, ETL generation
khác nhau, Sensor reschedule/fail-fast, ngày manual/backfill, biên timetable,
CSV Unicode/quote/newline, parse quarantine, tổng đơn, thiếu ID và ngưỡng churn.
Các test cleaning/RFM/anomaly legacy cũng chạy trong lệnh này.

`ruff check dags scripts tests` qua; formatter check cho code/test mới qua.
Test cấu trúc repository được gọi trực tiếp trên host và qua. `airflow dags
list-import-errors` trả `No data found`. Các provider đã pin theo image đang
chạy: Spark 6.3.2, standard 1.17.0, FAB 3.8.0; Airflow 3.3.1, PySpark 4.2.0.

## Lần chạy scheduler thực tế

Run kiểm chứng: `official_smoke_20260929`, business_date `2011-12-09`,
source_version `online-retail-v1`, k=2, churn_days=90.

Các stage đã hoàn tất có kết quả:

| Chỉ số | Kết quả |
|---|---:|
| Dòng CSV nguồn | 541.909 |
| Dòng trùng loại bỏ | 5.268 |
| Dòng audit sau dedup | 536.641 |
| Dòng hợp lệ cho RFM | 391.057 |
| Dòng lỗi parse | 0 |
| Khách hàng RFM | 4.334 |
| High-value (`M_score >= 4`) | 1.734 |
| Inactive (`Recency >= 90`) | 1.463 |
| Khách trong hai cụm KMeans | 1.663 / 2.671 |
| Hóa đơn được xét | 25.900 |
| Hóa đơn vượt ngưỡng thống kê | 133 |
| Hóa đơn không đủ điều kiện đánh giá | 5.993 |
| Dòng có ít nhất một cờ bất thường | 40.051 |

ETL mất 181,010 giây; RFM mất 125,751 giây theo stage manifest. Các thời gian
này gồm khởi tạo Spark và công việc trong stage, không gồm toàn bộ thời gian
queue/task startup. Tại thời điểm quan sát, Docker có giới hạn 13,55 GiB và
một container project khác dùng khoảng 5,56 GiB; pool được giữ ở một slot.

**Cả sáu task success**, chạy thực tế từ khoảng 13:17:59 đến 13:27:28 ngày
29/09/2026 (UTC+07:00). Audit mất 163,917 giây theo stage manifest. Completion
đã log `PIPELINE_COMPLETE` và công bố
`data/manifests/published/2011-12-10.json`. DAG được pause sau kiểm chứng.

Đọc lại tất cả tám output Parquet bằng một Spark session độc lập đã xác nhận
số dòng khớp metrics, cluster profile có hai cụm và giao dịch có đủ 13 tổ hợp
year/month. SHA-256 CSV nguồn vẫn khớp manifest bootstrap. Bằng chứng tổng hợp:
[official_run_20260929.json](evidence/official_run_20260929.json).

Đối chiếu trạng thái trực tiếp trên môi trường hiện tại:

```powershell
docker compose exec airflow-scheduler airflow tasks states-for-dag-run ecommerce_etl_dag official_smoke_20260929 -o json
docker compose exec airflow-scheduler airflow dags list-runs ecommerce_etl_dag -o json
```

Log nằm trong `logs/dag_id=ecommerce_etl_dag/run_id=official_smoke_20260929/`.
Kết quả đọc lại nằm tại `data/verification/official_run_readback.json`; helper
kiểm chứng local là `data/verification/verify_official_run.py`. Các đường dẫn
data/log bị ignore trong Git; file evidence trong docs chỉ chứa metadata và
số liệu tổng hợp. Đọc Graph/Grid tại <http://localhost:8080> để xem run này.

## Giới hạn cần trình bày khi bảo vệ

- Một slot pool chạy hai nhánh Spark lần lượt. Graph thể hiện hai nhánh độc lập,
  chưa phải bằng chứng thực thi song song; không tăng pool chỉ để có hình Gantt.
- Snapshot đọc toàn lịch sử đến cutoff, phù hợp dataset này; chưa benchmark
  hàng triệu dòng/ngày hoặc cluster nhiều máy.
- Deadline 60 phút đã được khai báo bằng API 3.3.1 và callback import được;
  chưa có bằng chứng cảnh báo thật sau một run vượt deadline. Không có watchdog
  độc lập cho scheduler chết hay run chưa được tạo. Hard timeout là cơ chế riêng.
- Unit test kiểm tra interval backfill; chưa chạy một chiến dịch backfill nhiều
  ngày hoặc fault injection dừng JVM trong lúc ghi Parquet trên Docker thật.
- Cluster profile hiện ghi số khách và mean R/F/M. Median, model centroids và
  model artifact là phần mở rộng của thiết kế, chưa được xuất trong phiên bản này.
- Inactivity là proxy theo quy tắc, anomaly là ứng viên rà soát; chưa có nhãn
  xác thực để tính accuracy/AUC hoặc precision/recall.
- Bằng chứng log và manifest được lưu local; slide và ảnh Graph/Grid cần được
  chọn để đưa vào bài nộp. Không tự coi toàn bộ T01–T28 hoặc toàn rubric đã đạt.
