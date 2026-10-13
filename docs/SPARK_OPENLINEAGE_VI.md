# Spark OpenLineage listener

Listener bổ sung lineage từ các truy vấn Spark đang chạy. Collector Airflow
tiếp tục mô tả các dataset nghiệp vụ và bước công bố; hai lớp bổ sung cho nhau.
Mặc định listener **tắt**; chỉ fixture được bật trong lần triển khai này.

## Luồng xử lý

```text
Airflow DAG run
  └─ Airflow task run (namespace retail-local)
       └─ Spark application (namespace retail-spark)
            └─ Spark SQL execution → input/output vật lý, schema, column lineage
```

1. `LineageSparkSubmitOperator` kiểm tra cờ bật, phiên bản PySpark và SHA-512
   của JAR trước khi submit. Không tải mạng trong task hoặc lúc parse DAG.
2. JAR được thêm vào `--jars` và driver classpath. Cách này tránh cảnh báo
   classloader của Spark 4 khi listener truy cập extension interface.
3. Spark provider đưa parent/root namespace, job name và run UUID vào cấu hình.
   UUID parent được tính bằng chính hàm provider dùng cho event task Airflow.
4. Listener thu sự kiện trong JVM của ETL, RFM, Anomaly và training. Mỗi lần
   submit ghi một file UUID riêng trong `logs/spark_openlineage/*.jsonl`.
5. RFM/Anomaly vẫn phải qua publication gate trước khi task thành công và phát
   Asset. Notify vẫn đối soát hai nhánh; Dashboard vẫn chỉ nhận retail snapshot.
   Retry sử dụng publication marker có sẵn không chạy lại Spark, nên không tạo
   thêm Spark run giả.

**Spark COMPLETE không đồng nghĩa dataset đã được công bố nghiệp vụ.** Một
query có thể đã ghi Parquet thành công nhưng bước kiểm tra manifest phía sau
thất bại. Quyết định cho consumer chạy vẫn thuộc Asset event của Airflow.

## Phiên bản và cấu hình

| Thành phần | Phiên bản kiểm tra |
|---|---|
| Airflow / Spark provider | 3.3.1 / 6.3.2 |
| OpenLineage Airflow provider / Python client | 2.20.0 / 1.52.0 |
| PySpark / Scala binary | 4.2.0 / 2.13 (scala-library 2.13.18) |
| Java driver | OpenJDK 17.0.20.1 |
| Spark listener | `io.openlineage:openlineage-spark_2.13:1.52.0` |
| Marquez | 0.51.1 |

JAR và checksum được ghim trong `scripts/retail_spark_lineage.py`. JAR nằm
trong `data/lineage/jars/`, được Git ignore; sử dụng volume data sẵn có.
Không thay image, không thêm Spark service hoặc tăng số slot của pool.
Nâng PySpark phải kiểm tra lại listener; operator hiện từ chối phiên bản khác.

| Biến | Mặc định | Tác dụng |
|---|---|---|
| `RETAIL_SPARK_LINEAGE_ENABLED` | `false` | Bật listener khi submit Spark |
| `RETAIL_SPARK_LINEAGE_MARQUEZ` | `false` | Gửi thêm tới Marquez local |
| `OPENLINEAGE_DISABLED` | `false` | Compose ánh xạ sang cấu hình Airflow; `true` tắt cả hai lớp |

File-only dùng Java `transport.location`; composite dùng transport local và
HTTP đến `http://marquez:5000`, timeout 3 giây, `continueOnFailure=true`.
Không sao chép cấu hình transport Python vào Java vì tên thuộc tính khác nhau.
Các cờ phải là `true` hoặc `false`; gõ sai, thiếu JAR hoặc checksum sai sẽ làm
task thất bại trước khi chạy Spark khi đã bật tính năng. Tắt cờ listener để
khôi phục cách chạy cũ; không bỏ qua kiểm tra chất lượng dữ liệu.

## Cài đặt và bật

Tại thư mục gốc repository:

```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/retail_spark_lineage.py
```

Lệnh tải một lần từ Maven Central qua HTTPS và kiểm tra checksum trước khi
đổi tên file tạm. Chạy lại chỉ xác minh JAR đã có, không tải lại.

Để chỉ ghi file, đặt `RETAIL_SPARK_LINEAGE_ENABLED=true` trong `.env` và
`RETAIL_SPARK_LINEAGE_MARQUEZ=false`. Nếu đang sử dụng Marquez như cấu hình
local hiện tại, recreate bằng cả hai Compose file để giữ transport Airflow:

```powershell
docker compose -f docker-compose.yaml -f docker-compose.lineage.yaml up -d --no-build --no-deps airflow-scheduler airflow-dag-processor airflow-apiserver
```

Để Spark cũng gửi tới Marquez, đặt thêm `RETAIL_SPARK_LINEAGE_MARQUEZ=true`,
bảo đảm các service Marquez đang chạy theo [hướng dẫn](OPENLINEAGE_VI.md),
rồi recreate như trên. Trên máy chỉ dùng cấu hình base, dùng `docker compose`
với file mặc định. Thay cờ không tự kích hoạt DAG đang paused.

Tắt listener: đặt `RETAIL_SPARK_LINEAGE_ENABLED=false`, recreate đúng bộ
Compose đang sử dụng. Giữ JAR và log để có thể bật lại; không xóa volume.
Lần triển khai này không sửa `.env` thật và không recreate service production.

## Kiểm tra nhẹ

```powershell
docker compose exec airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_spark_lineage.py tests/test_branch_publication.py tests/test_rfm_publication.py tests/test_retail_dag.py tests/test_consumer_events.py tests/test_retail_lineage.py
docker compose exec airflow-scheduler python /opt/airflow/tests/spark_lineage_smoke.py --marquez
docker compose exec airflow-scheduler python /opt/airflow/tests/spark_lineage_smoke.py --broken-http
docker compose exec airflow-scheduler python /opt/airflow/tests/spark_airflow_lineage_smoke.py
```

Chạy tuần tự: mỗi fixture chỉ dùng `local[1]`, driver 768 MiB, bốn dòng CSV,
Parquet riêng dưới `logs/`; không đọc dữ liệu retail, không train model.
Bỏ `--marquez` để thử chế độ file-only mà không cần backend.

- Fixture Spark kiểm tra kết quả tính toán, START/COMPLETE cùng run ID,
  CSV input, Parquet output, schema, column lineage và parent được truyền vào.
- `--marquez` đối chiếu đúng Spark SQL run trên API với trạng thái COMPLETED.
  Mở UI http://localhost:3000, namespace `retail-spark`; job fixture mang tên
  `retail_spark_lineage_fixture` và các execution con.
- `--broken-http` cố tình gửi tới port local không phục vụ; job phải thành
  công và file lineage vẫn có dữ liệu. Log cảnh báo transport là có chủ đích.
- Fixture Airflow sử dụng `dag.test()` trong bundle riêng, kiểm tra parent
  trong Spark event bằng UUID của event task Airflow thực tế. Fixture DAG
  `retail_spark_lineage_smoke` có schedule=None và paused.
- Bằng chứng JSON nằm trong `logs/spark_lineage_fixture/<uuid>/verification.json`
  và `logs/spark_airflow_fixture/<uuid>/verification.json`; log được Git ignore.

## Bằng chứng ngày 13/10/2026

- Bộ kiểm thử trên: **61 passed in 14.63s**; Ruff đạt. Bốn DAG import và
  serialize thành công, đều paused; không có run running/queued sau fixture.
- Spark + Marquez: 7 events, write run
  `01a1355a-1936-71b6-8a09-0b27c2d01d76` ở trạng thái COMPLETED.
  Bằng chứng: `logs/spark_lineage_fixture/98011d0a-e788-4bfc-8a1d-82aa407b50e7/`.
- HTTP không hoạt động: phép tính và file lineage vẫn đạt, 7 events.
  Bằng chứng: `logs/spark_lineage_fixture/34b39494-c07d-4826-ab4d-2a426cab74c9/`.
- DAG fixture Airflow thành công; Spark START/COMPLETE có parent task UUID
  `01a135a1-5fa1-770f-bdc7-b75774fe7655`, khớp event Airflow.
  Bằng chứng: `logs/spark_airflow_fixture/1ecb098d-a9bb-4265-b2fc-61d5462089e2/`.
- Lần thử đầu phát hiện cảnh báo classloader, đã sửa bằng driver classpath;
  lỗi kiểm chứng API Marquez và so sánh toàn bộ job facet cũng đã sửa.
  Các bằng chứng đạt phía trên thuộc các lần kiểm tra sau sửa.

## Phạm vi và giới hạn

- Fixture chứng minh `revenue ← qty, price`, `customer ← customer`, cùng ảnh
  hưởng của filter/group-by. Không suy ra toàn bộ UDF, Spark ML/KMeans, RDD
  hoặc mọi rule Anomaly đều có column lineage đầy đủ.
- Spark trả namespace vật lý `file`, đường dẫn generation thực tế. Extractor
  Airflow dùng namespace logic `file://`, có facet `physical_path`. Chúng chưa
  tự hợp nhất thành cùng node dataset trong Marquez. Parent run nối hai lớp
  công việc; facet hỗ trợ đối chiếu dataset thủ công.
- Tắt facet `spark_logicalPlan` để hạn chế kích thước metadata; listener vẫn
  phân tích query plan trong bộ nhớ để tạo schema và column lineage.
- Listener không quét lại Parquet, nhưng tăng xử lý metadata trong JVM và số
  event theo query. Chưa đo overhead trên toàn dataset. Pool hiện vẫn một slot.
- Giao nhận best effort: transport lỗi có thể mất event, HTTP timeout không
  phải cam kết tổng thời gian; chưa có outbox, replay hoặc retention tự động.
  File vẫn lưu khi HTTP lỗi trong fixture, nhưng không đảm bảo khi ổ đĩa đầy.
- Chưa chạy lại pipeline retail đầy đủ với listener. Bằng chứng retail ngày
  11/10 không được dùng làm bằng chứng Spark lineage mới.

Tham khảo: [cài đặt listener](https://openlineage.io/docs/integrations/spark/installation/),
[Java/Spark transport](https://openlineage.io/docs/integrations/spark/configuration/transport/),
[Airflow parent propagation](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/2.20.0/spark.html).
