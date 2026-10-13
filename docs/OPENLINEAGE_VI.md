# OpenLineage: metadata từ Airflow và Marquez local

Đã bật thu thập ở Airflow, ghi từng event vào
`logs/openlineage-<timestamp>.json`. Cấu hình mặc định chỉ ghi file; override
`docker-compose.lineage.yaml` bổ sung Marquez API/UI và gửi HTTP song song.
Collector Airflow không quét Parquet hay chạy Spark để tạo lineage.
Đã bổ sung Spark listener tùy chọn (mặc định tắt): thu metadata ngay trong
JVM khi job chạy, có parent run nối về task Airflow. Xem cấu hình, fixture và
giới hạn tại [Spark OpenLineage listener](SPARK_OPENLINEAGE_VI.md).

## Bước 2: bật giao diện Marquez khi cần

Chạy tại thư mục gốc repository trong PowerShell:

```powershell
docker compose -f docker-compose.yaml -f docker-compose.lineage.yaml up -d --no-build marquez-db marquez marquez-web
docker compose -f docker-compose.yaml -f docker-compose.lineage.yaml up -d --no-build --no-deps airflow-scheduler airflow-dag-processor airflow-apiserver
```

Mở **http://localhost:3000**, chọn namespace `retail-local`, job
`retail_lineage_smoke.metadata_only` để xem `input → job → output` sau smoke-test.
API tại http://localhost:5000/api/v1/namespaces. UI có thể cần vài giây sau
container startup để API hoàn thành migration database.

- Ghim Marquez API/UI `0.51.1`; database PostgreSQL 16 riêng, giữ trong named
  volume `marquez-db-volume`. Không dùng chung database Airflow.
- Giới hạn RAM: API 768 MiB (Java heap 512 MiB), UI 256 MiB, DB 256 MiB;
  tổng trần 1,25 GiB, không phải lượng RAM luôn sử dụng. CPU tương ứng 1/0,5/0,5.
- Tắt advanced search, không triển khai OpenSearch. Port chỉ bind `127.0.0.1`;
  cấu hình tài khoản DB dành cho demo local. Các service không tự restart.
- Composite transport ghi file local và gửi HTTP đến `marquez:5000` trong
  mạng Docker. HTTP timeout 3 giây, không retry. Marquez ngừng hoạt động vẫn
  giữ được bản ghi local; file không tự được gửi bù khi Marquez trở lại.
- Schema custom vẫn dùng URI local; Marquez lưu facet JSON, chưa có giao diện
  chuyên biệt cho `retail_provenance` và `retail_snapshot`.

Kiểm tra nhẹ sau khi API sẵn sàng:

```powershell
docker compose exec airflow-scheduler python /opt/airflow/tests/lineage_smoke.py
docker compose exec airflow-scheduler python /opt/airflow/tests/verify_marquez.py
```

Script thứ hai đối chiếu runId mới nhất trong file local với run COMPLETED
trên Marquez, custom facet, input/output và đồ thị lineage; lưu bằng chứng
`logs/marquez-verification.json`. Đây là dữ liệu fixture, không phải bằng
chứng chạy lại pipeline retail. Event cũ không được tự động nhập vào Marquez.

Tắt Marquez và trả Airflow về chế độ chỉ ghi file để giải phóng RAM:

```powershell
docker compose up -d --no-build --no-deps airflow-scheduler airflow-dag-processor airflow-apiserver
docker compose -f docker-compose.yaml -f docker-compose.lineage.yaml stop marquez-web marquez marquez-db
```

Không dùng `down -v`: volume giữ lịch sử để mở lại lần sau. Khi cần HTTP,
luôn dùng cả hai file với lệnh `up` cho Airflow; `up` chỉ với file mặc định
sẽ đưa collector về chế độ file. Các DAG production giữ paused.

Tham khảo cấu hình chính thức:
[Marquez Compose](https://github.com/MarquezProject/marquez/blob/0.51.1/docker-compose.yml)
và [Marquez Web](https://github.com/MarquezProject/marquez/blob/0.51.1/docker-compose.web.yml).

Đã xác minh bước 2 ngày 11/10/2026: smoke DAG thành công (~20 giây), run
`01a12afe-c21f-7ad8-9783-8c46db89e86b` có trạng thái COMPLETED trong Marquez;
custom facet được giữ và graph có đủ 3 node. API/UI trả HTTP 200 từ host.
RAM đo sau kiểm tra: API ~193 MiB, UI ~19 MiB, DB ~73 MiB (tổng ~285 MiB).
Backend/UI đang bật để xem; cả 5 DAG vẫn paused, không có run running/queued.

Provider/client được ghim theo image đã kiểm tra: OpenLineage provider
`2.20.0`, client `1.52.0`, Airflow `3.3.1`. Image hiện có đã chứa đúng các
package này; bước triển khai chỉ recreate ba service Airflow bằng `--no-build`.
Khi build image mới, `requirements.txt` giữ các phiên bản đó.

## Dữ liệu thu thập

`dags/retail_support/lineage.py` cung cấp extractor cho PythonOperator,
SparkSubmitOperator, LineageSparkSubmitOperator, RfmSparkSubmitOperator,
AuditSparkSubmitOperator và LandingValidationSensor. Provider quản lý event
lifecycle, timestamp, run ID và quan hệ DAG/task; extractor đọc JSON manifest
của đúng `dag_id`/`run_id` để bổ sung input/output và metadata.

| Task | Quan hệ dataset |
|---|---|
| validate_raw_data | CSV gốc → landing của source_version |
| submit_pyspark_etl | Landing → staged transactions, audit_input, parse_errors |
| compute_rfm_metrics | Staged transactions + model pipeline đã ghim → published RFM customers + staged cluster profile (sau gate công bố RFM) |
| detect_anomalies | Staged audit_input → published flagged_orders + staged row/order assessments (sau gate Audit) |
| notify_completion | Staged transactions + published RFM/anomaly → published transactions + retail_snapshot (completion manifest) |
| validate_training_input | Đọc generation ETL đã ghim |
| train_rfm_model | Transactions → candidate model manifest |
| publish_model | Candidate đã kích hoạt trong registry → model pipeline |
| Marketing / Dashboard | Đọc published RFM / retail_snapshot tương ứng; không khai báo đầu ra giả |

Namespace dataset là `file://`; tên logic là đường dẫn tuyệt đối ổn định dưới
data root. Nhóm `staged/...` là định danh logic, không phải thư mục vật lý mới.
Facet `retail_snapshot.physical_path` chỉ rõ generation thực tế.
Model dùng đường dẫn pipeline theo phiên bản làm định danh.

Facet `retail_provenance` chứa run_key, cutoff (`run_date`), code_version,
source_version/source_hash, context_hash; thêm etl_hash, model_version, metrics
và completion_manifest khi phù hợp. Metrics lấy từ manifest, không tính lại.
Schema custom nằm tại `dags/retail_support/schemas/retail-lineage-v1.json`;
schema URI hiện là file local trong container, chưa phải schema HTTP công khai.

Theo kiến trúc cập nhật 12/10/2026, mỗi nhánh chỉ có published output sau
COMPLETE với publication marker tương ứng. notify_completion nhận RFM và
Anomaly đã published làm input, chỉ công bố curated và retail_snapshot.
Retail snapshot là dataset logic ánh xạ tới completion.json của run, không
phải một bản Parquet mới. Extractor đăng ký cả hai subclass SparkSubmit.
START/FAIL không
quảng bá outputs, kể cả khi retry còn manifest cũ. Khi metadata thiếu/hỏng,
extractor ghi warning và bỏ phần lineage đó; khi transport lỗi, provider ghi
warning. Không biến lỗi quan sát thành lỗi xử lý dữ liệu, nhưng event có thể
bị mất: bước này chưa có outbox/replay đảm bảo giao nhận.

Consumer được nối qua cùng định danh dataset published. Chưa bổ sung custom
parent-run facet nối consumer với chính xác producer run; metadata nguồn vẫn
có trong Airflow Asset events và bằng chứng chạy trước. Đây là lineage dataset
do ứng dụng khai báo. Listener tùy chọn bổ sung lineage từ Spark query plan
và từng cột cho các phép toán được hỗ trợ; chưa chạy lại retail đầy đủ.

## Vận hành nhẹ

- Bốn DAG dữ liệu vẫn paused. Không cần unpause để đọc JSON events đã ghi.
- `OPENLINEAGE_DISABLED=false` bật collector; đặt `true` trong `.env` rồi
  recreate service để tắt. Không sửa `.env` trong lần triển khai này.
- Cấu hình tắt thu thập source code/full task info. Parent injection vào Spark
  chỉ bật trong operator khi listener được bật; Java transport cấu hình riêng.
  Collector Airflow mặc định chỉ ghi file; override gửi thêm đến Marquez local.
- `append=false` tạo file event riêng theo timestamp. Thư mục logs đã bị
  Git ignore; bước này chưa cấu hình retention tự động.

Áp dụng cấu hình với image đã có package:

```powershell
docker compose up -d --no-build --no-deps airflow-scheduler airflow-dag-processor airflow-apiserver
```

## Xác minh ngày 11/10/2026

```powershell
docker compose exec airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_retail_lineage.py tests/test_retail_dag.py tests/test_consumer_events.py
docker compose exec airflow-scheduler python /opt/airflow/tests/lineage_smoke.py
```

- 21 tests đạt: publication gate, model input/activation, metadata không hợp
  lệ, provider registration/fallback, file serialization/schema và lỗi transport.
- Smoke-test một task bằng `dag.test()` trong bundle fixture riêng thành công
  khoảng 15 giây. Đã kiểm tra START/COMPLETE cùng runId, namespace `retail-local`,
  input/output fixture và custom facet trong file thực tế. Log:
  `logs/openlineage-smoke.log`. Fixture DAG `retail_lineage_smoke` paused,
  schedule=None; không thuộc bundle DAG dữ liệu.
- Đọc lại manifest thật của 8 task training/daily đã chạy ngày 11/10: extractor
  trả đúng số lượng input/output, không phát lại events lịch sử.
- Không chạy lại dataset đầy đủ, không train lại model và không chạy test Spark.
  Chưa có bằng chứng OpenLineage của một lần chạy production mới từ đầu đến cuối.

Các bằng chứng 11/10 trên thuộc thời điểm trước khi tách điểm phát RFM.
Kiểm thử mới trong `test_retail_lineage.py` kiểm tra mỗi nhánh COMPLETE không cần
nhánh kia/completion, notify không phát lại Asset nhánh, Dashboard chỉ đọc
retail_snapshot và START/FAIL/fallback của cả ba task không quảng bá output. Chưa chạy lại retail với collector sau thay đổi.

Tham khảo API:
[Airflow custom extractors](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/2.20.0/guides/developer.html).
