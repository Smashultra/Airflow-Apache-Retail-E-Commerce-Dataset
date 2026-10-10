# Hướng dẫn vận hành PySpark RFM và ghép DAG

Cập nhật: 2026-10-10. Tài liệu đối chiếu với code hiện tại trong `scripts/`.
Dành cho người nhận phần RFM để tích hợp vào DAG daily và training định kỳ.
Các lệnh dưới đây là hướng dẫn chạy, chưa phải bằng chứng đã chạy thành công end-to-end.

## 1. Nhận đúng bộ file

| File | Vai trò |
|---|---|
| `scripts/pyspark_rfm.py` | Helper dùng chung, ingest store, tính RFM, daily load model và phân cụm |
| `scripts/train_rfm_kmeans.py` | Import helper ở trên, fit và lưu model mới |
| `tests/test_pyspark_rfm.py` | Kiểm thử hợp đồng dữ liệu, CLI, RFM, model và store |
| `Dockerfile`, `requirements.txt`, `docker-compose.yaml` | Môi trường chạy và mount dữ liệu/scripts |

Không lấy `.agent/backups/rfm_cli_20261008/pyspark_rfm.py` làm source triển khai:
đó là bản trước khi đơn giản hóa CLI, suy ra bucket count và bắt buộc input daily.
Tài liệu cũ `RFM_INCREMENTAL_VI.md` còn ví dụ daily không có input; ví dụ đó không còn hợp lệ.

**DAG hiện có chưa gọi được trực tiếp CLI này.** `dags/ecommerce_etl_dag.py`
truyền `--pipeline-context`; cả hai entry point RFM hiện tại không nhận tham số đó.
Daily cũng không nhận `--k`. Người ghép phải sửa hợp đồng gọi task và phần kiểm tra/
publish downstream; không chỉ đổi tên script trong helper `spark_job` hiện tại.

## 2. Luồng vận hành

```text
Lần đầu:
Curated lịch sử sạch -> training --initialize-store -> store + model v1
                                                    |
Daily:                                              v
Batch curated mới -> ingest vào cùng store -> RFM tích lũy -> load model v1
                                                         -> output theo run_date

Định kỳ:
Store đã nạp đủ dữ liệu -> training chỉ đọc -> model v2
                           kiểm tra đạt -> DAG/config chọn v2 cho các run sau
```

- `pyspark_rfm.py`: chỉ load/transform, không fit lại scaler hoặc KMeans.
- `train_rfm_kmeans.py`: fit pipeline gồm vector đầu vào, StandardScaler và KMeans.
  Features dùng log RFM; các điểm `R_score/F_score/M_score` không phải đầu vào KMeans.
- Lịch monthly là trách nhiệm của DAG; script training không tự lên lịch hoặc tự promote model.
- Store chia khách vào bucket cố định, lưu giao dịch theo ngày, invoice ledger và state.
  Batch mới chỉ cập nhật ngày/bucket liên quan; manifest công bố snapshot sau khi ghi xong.
  Không đọc store bằng cách glob toàn bộ thư mục vì có nhiều thế hệ dữ liệu.

## 3. Input, cutoff và ý nghĩa output

Input là **thư mục Parquet đã clean**, không phải CSV raw. Năm cột bắt buộc:

| Cột | Spark type | Yêu cầu |
|---|---|---|
| CustomerID | string | Không null/rỗng |
| InvoiceNo | string | Không null/rỗng; giao dịch hủy phải được loại ở cleaning |
| InvoiceDate | timestamp | Không null; trước cutoff |
| Quantity | int | Dương |
| UnitPrice | double | Dương, hữu hạn |

Giữ schema nghiệp vụ ổn định giữa các batch, kể cả các cột bổ sung như StockCode,
Description. `year/month` được chuẩn hóa, không thuộc khóa chống trùng;
`transaction_date` và `_customer_bucket` là tên dành riêng, không được có trong input.
Cleaning chịu trách nhiệm loại dòng không hợp lệ/phí dịch vụ theo quy tắc dự án.

`--run-date D` là **00:00 UTC của D, loại trừ D**. Ví dụ xử lý ngày 2011-12-09:
cutoff là `2011-12-10`, cửa sổ khách hoạt động mặc định `[2011-12-09, 2011-12-10)`.
Không dùng ngày đồng hồ hiện tại thay cho ngày dữ liệu khi backfill. Input chứa
`InvoiceDate >= cutoff` bị từ chối, không tự lọc bỏ; hãy chuẩn bị batch đúng cutoff.

RFM của mỗi khách được tính trên lịch sử tích lũy trong snapshot:

- Recency: số ngày từ ngày mua gần nhất tới cutoff.
- Frequency: số InvoiceNo khác nhau của khách, không phải số dòng sản phẩm.
- Monetary: cộng Quantity × UnitPrice bằng decimal (cast mỗi toán hạng về decimal(18,4)),
  giữ tổng chưa làm tròn trong state và làm tròn 2 chữ số khi xuất RFM.
- Điểm R/F/M: percent_rank trên toàn bộ khách trong state, rồi mới lọc khách hoạt động.
  R thấp tốt hơn, F/M cao tốt hơn; metric đồng hạng toàn bộ nhận điểm 1.
- Mặc định chỉ xuất khách có mua trong cửa sổ hoạt động, nhưng RFM của họ vẫn tích lũy.
  `--period-start` mở rộng cửa sổ; `--all-customers` xuất mọi khách trong snapshot.
- Cluster là ID số từ model; không mặc định mang nghĩa VIP/churn. ID giữa hai model
  không nhất thiết tương đương. Daily có thể chỉ xuất một phần số cụm đã train.

Output: `<output>/run_date=YYYY-MM-DD/`, gồm CustomerID, LastPurchaseDate,
Recency, Frequency, Monetary, R_score, F_score, M_score, RFM_score, Cluster,
model_version. `run_date` là cột partition: đọc từ output root để Spark suy ra cột này;
nếu chỉ đọc thư mục ngày thì đặt `basePath` là output root hoặc tự bổ sung ngày.
Rerun ghi đè đúng partition ngày đó, giữ các ngày khác. Output không phải giao dịch
nguyên tử chung với store: store có thể đã commit trong khi scoring/output thất bại.

## 4. Tham số cần truyền

| Tham số | Daily | Training |
|---|---|---|
| `--input` | Bắt buộc, curated batch | Tùy chọn; bỏ để chỉ đọc store |
| `--batch-id` | Bắt buộc | Bắt buộc khi có input |
| `--run-date` | Bắt buộc, YYYY-MM-DD | Bắt buộc, YYYY-MM-DD |
| `--store-path` | Mặc định `<project>/data/curated/rfm_store` | Cùng mặc định |
| `--initialize-store` | Chỉ khi khởi tạo store | Thường dùng khi bootstrap lần đầu |
| `--output` | Mặc định `<project>/data/rfm` | Không nhận |
| `--model-path` | Bắt buộc, model đã fit | Không nhận |
| `--model-version` | Tùy chọn, mặc định tên thư mục model | Không nhận |
| `--period-start` | Mặc định D−1, phải trước D | Không nhận |
| `--all-customers` | Tùy chọn | Không nhận; training dùng population |
| `--k` | Không nhận; lấy từ model | Bắt buộc, ít nhất 2 |
| `--seed` | Không nhận | Mặc định 42 |
| `--model-output` | Không nhận | Bắt buộc, đường dẫn mới chưa tồn tại |

Trong DAG nên truyền rõ các đường dẫn tuyệt đối bên trong container. Không còn flag
`--num-buckets`: store mới mặc định 64; CLI tự dùng bucket count của store đã có.
Batch ID dài 1–128 ký tự, bắt đầu bằng chữ/số, chỉ gồm chữ/số/`_`/`-`.

## 5. Chạy thủ công bằng PowerShell và Docker

Chạy từ repository trên máy host. Docker Desktop phải hoạt động và image đã build.
Compose mount `scripts` vào `/opt/airflow/scripts`, `data` vào `/opt/airflow/data`.
Các lệnh dùng service `airflow-scheduler` làm container chạy Spark một lần;
`--no-deps` không khởi động các service Airflow/Postgres phụ thuộc.
Repo hiện pin PySpark 4.2.0, Spark provider 6.3.2 và cài Java 17 trong Dockerfile.

```powershell
docker compose build airflow-scheduler
docker compose run --rm --no-deps airflow-scheduler spark-submit --version
docker compose run --rm --no-deps airflow-scheduler python /opt/airflow/scripts/pyspark_rfm.py --help
docker compose run --rm --no-deps airflow-scheduler python /opt/airflow/scripts/train_rfm_kmeans.py --help
```

### Bước A — Bootstrap lịch sử và model đầu tiên

Ví dụ giả định `data/curated/RFM.parquet` đã tồn tại, không rỗng, đúng schema và
chỉ chứa giao dịch trước 2011-12-10. Đây là demo trên lịch sử, không phải đánh giá
model ngoài mẫu. `k=4` là ví dụ: cần chọn k bằng phân tích và đủ khách/vector khác nhau.
Store `rfm_store_handoff` và model `handoff_v1` phải là đường dẫn mới cho lần đầu.

```powershell
docker compose run --rm --no-deps airflow-scheduler spark-submit --master "local[2]" --driver-memory 3g --py-files /opt/airflow/scripts/pyspark_rfm.py /opt/airflow/scripts/train_rfm_kmeans.py --input /opt/airflow/data/curated/RFM.parquet --store-path /opt/airflow/data/curated/rfm_store_handoff --batch-id seed_before_2011_12_10 --initialize-store --run-date 2011-12-10 --k 4 --seed 42 --model-output /opt/airflow/data/models/rfm_kmeans/handoff_v1
```

Model lưu cả pipeline. Audit ở `<model-output>/rfm_training_info/` là thư mục text
có part file JSON ghi cutoff, k, seed, store_version, store_path và log_features.
Đợi task training hoàn tất cả model lẫn audit trước khi dùng model.

### Bước B — Demo daily bằng chính batch đã bootstrap

CLI daily luôn cần input. Dùng lại **đúng input, batch-id, cutoff** ở bước A sẽ
đọc snapshot của batch đã commit, không cộng đôi lịch sử. Không cần initialize lại.

```powershell
docker compose run --rm --no-deps airflow-scheduler spark-submit --master "local[2]" --driver-memory 3g /opt/airflow/scripts/pyspark_rfm.py --input /opt/airflow/data/curated/RFM.parquet --store-path /opt/airflow/data/curated/rfm_store_handoff --batch-id seed_before_2011_12_10 --run-date 2011-12-10 --output /opt/airflow/data/rfm_handoff --model-path /opt/airflow/data/models/rfm_kmeans/handoff_v1 --model-version handoff_v1
```

Kết quả là khách mua ngày 2011-12-09 với RFM tích lũy. Thêm `--all-customers` nếu
muốn kiểm tra toàn bộ khách; việc này ghi đè cùng partition nếu giữ nguyên output.

### Bước C — Daily khi có batch mới

Ví dụ sau **chỉ chạy khi đã có** `data/curated/batch_2011_12_10.parquet`, đúng schema,
không rỗng và chứa giao dịch trước 2011-12-11. Không giả định dataset gốc có ngày này.
Upstream phải giữ batch bất biến để retry dùng lại được cùng nội dung.

```powershell
docker compose run --rm --no-deps airflow-scheduler spark-submit --master "local[2]" --driver-memory 3g /opt/airflow/scripts/pyspark_rfm.py --input /opt/airflow/data/curated/batch_2011_12_10.parquet --store-path /opt/airflow/data/curated/rfm_store_handoff --batch-id day_2011_12_10 --run-date 2011-12-11 --output /opt/airflow/data/rfm_handoff --model-path /opt/airflow/data/models/rfm_kmeans/handoff_v1 --model-version handoff_v1
```

### Bước D — Training định kỳ từ store

Chỉ chạy sau khi upstream xác nhận đã nạp đủ dữ liệu trước cutoff. Nếu chưa có dữ
liệu mới, lệnh này chỉ train lại trên lịch sử cũ. Script không kiểm chứng độ đầy đủ.

```powershell
docker compose run --rm --no-deps airflow-scheduler spark-submit --master "local[2]" --driver-memory 3g --py-files /opt/airflow/scripts/pyspark_rfm.py /opt/airflow/scripts/train_rfm_kmeans.py --store-path /opt/airflow/data/curated/rfm_store_handoff --run-date 2012-01-01 --k 4 --seed 42 --model-output /opt/airflow/data/models/rfm_kmeans/handoff_v2
```

Không có input/batch-id ở bước D: chọn snapshot mới nhất có cutoff ≤ ngày yêu cầu,
tính lại Recency tại cutoff rồi fit. Sau kiểm tra, cấu hình các run daily tiếp theo
trỏ tới v2. Giữ model path/version cố định trong suốt retry của từng run.

## 6. Ghép vào DAG

Giữ Spark computation trong `scripts/`, dùng `SparkSubmitOperator` để gọi job.
Ví dụ task bên dưới đặt **bên trong DAG đã khai báo**, không phải DAG hoàn chỉnh.
`params` phải được DAG định nghĩa/kiểm tra trước với đường dẫn, ngày và batch đã đóng băng.

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

rfm_daily = SparkSubmitOperator(
    task_id="rfm_daily",
    application="/opt/airflow/scripts/pyspark_rfm.py",
    conn_id="spark_default",
    deploy_mode="client",
    driver_memory="3g",
    pool="retail_spark",
    pool_slots=1,
    conf={"spark.sql.session.timeZone": "UTC"},
    application_args=[
        "--input", "{{ params.rfm_input }}",
        "--batch-id", "{{ params.rfm_batch_id }}",
        "--run-date", "{{ params.rfm_cutoff }}",
        "--store-path", "/opt/airflow/data/curated/rfm_store_handoff",
        "--output", "/opt/airflow/data/rfm_handoff",
        "--model-path", "{{ params.rfm_model_path }}",
        "--model-version", "{{ params.rfm_model_version }}",
    ],
)
```

Connection `spark_default` trong Compose đang dùng `local[2]`. Tạo pool
`retail_spark` với **1 slot** nếu chưa có; mọi task RFM ghi chung tài nguyên phải
dùng pool này. Đặt `max_active_runs=1` cho daily để hạn chế chồng run, nhưng vẫn
cần pool/dependency chung khi có nhiều DAG. Chạy ingest theo thứ tự cutoff tăng dần;
giới hạn concurrency một mình không đảm bảo đúng thứ tự backfill.

Task training dùng cùng operator, đổi `application` sang `train_rfm_kmeans.py`,
thêm `py_files="/opt/airflow/scripts/pyspark_rfm.py"`; args định kỳ gồm
`--store-path`, `--run-date`, `--k`, `--seed`, `--model-output` như bước D.

Quan hệ task cần thiết:

1. Daily: xác nhận batch sẵn sàng → cleaning ra batch bất biến → RFM → kiểm tra output → publish.
2. Training: xác nhận các batch trước cutoff đã ingest → training → kiểm tra model/audit → cập nhật model active.
3. Model đầu tiên được bootstrap trước daily. Các kỳ sau có thể ingest/score batch
   cuối kỳ bằng model cũ rồi train model mới; tránh vòng phụ thuộc daily đợi model mới
   trong khi training lại đợi daily ingest.

Với DAG có khoảng dữ liệu UTC `[D, D+1)`, truyền ngày `D+1` làm cutoff, không mặc
định dùng `ds` là ngày xử lý. Với manual run, nhận cutoff tường minh và kiểm tra.
Batch ID gắn với dữ liệu nghiệp vụ, không gắn `try_number`. Freeze input, cutoff,
batch ID và model version để retry không tự đổi sang model vừa được promote.

Publisher hiện tại của DAG dựa trên context/manifests riêng. Cần ánh xạ lại đường
dẫn output, schema, số dòng và version model theo hợp đồng trên; không đánh dấu
publish thành công chỉ vì store đã có manifest. Chưa có adapter tự động trong guide này.

## 7. Retry, lỗi và giới hạn cần xử lý ở DAG

| Tình huống | Cách xử lý |
|---|---|
| Scoring lỗi sau ingest | Retry đúng input/ID/cutoff/model; ingest trả snapshot đã commit |
| Cùng ID nhưng khác nội dung/cutoff | Dừng, kiểm tra upstream; không thay ID để che lỗi dữ liệu |
| Batch mới có cutoff nhỏ hơn store hiện tại | Bị từ chối; replay batch đã commit hoặc rebuild store riêng theo lịch sử đúng thứ tự |
| Store bị lock sau crash | Xác minh không còn writer, kiểm tra trạng thái trước khi phục hồi thủ công; không tự xóa lock trong retry |
| Model-output đã tồn tại | Training không overwrite; kiểm tra lần chạy trước, chọn version mới khi cần chạy lại |
| Model lưu xong nhưng audit thất bại | Đường dẫn model có thể tồn tại dù task fail; chưa promote, kiểm tra rồi chọn attempt/version mới |
| Input Parquet rỗng | Bị từ chối trước ingest; DAG cần chính sách ngày không giao dịch, không coi đây là output rỗng được hỗ trợ |
| Không có khách hoạt động trong cửa sổ nhưng input hợp lệ | Daily ghi partition rỗng đúng schema và không load model; thành công này không chứng minh model tồn tại/hợp lệ |
| Late arrival của ngày cũ | State được cập nhật ở cutoff hiện tại; muốn xuất khách ngày cũ thì mở rộng period-start hoặc all-customers |
| Hai task ghi cùng output ngày/model path | Không được chạy đồng thời; store lock không bảo vệ hai loại output này |

Store chống trùng trên toàn bộ dòng nghiệp vụ; một hóa đơn nhiều dòng/batch chỉ
được tính Frequency một lần cho mỗi khách. Hai giao dịch thực sự khác nhau nhưng
giống hệt mọi cột có thể bị gộp. Chưa hỗ trợ sửa/xóa/upsert: thay giá/số lượng thành
dòng khác, không tự thay thế dòng cũ. Cần thiết kế điều chỉnh hoặc rebuild store mới.

Backend hỗ trợ local/HDFS, một writer; không phải giao thức giao dịch cho S3/object
storage. Snapshot/file tăng dần, chưa có compaction/GC tự động. Global percentile
vẫn scan/sort state toàn bộ khách; không cam kết chi phí chỉ tỷ lệ với batch mới.
Monthly chọn cutoff-safe snapshot nhưng không đảm bảo lịch sử đã nạp đủ.

## 8. Kiểm chứng trước khi đưa vào lịch chạy

```powershell
docker compose run --rm --no-deps airflow-scheduler python -m pytest -q /opt/airflow/tests/test_pyspark_rfm.py
```

Sau test, chạy A → B trên các đường dẫn thử riêng và kiểm tra:

1. Store có manifest commit; model có pipeline và audit metadata đúng cutoff/version.
2. Output có `_SUCCESS`, schema đúng và số khách đúng cửa sổ; đọc từ output root để có run_date.
3. Chạy lại B với cùng cấu hình: R/F/M và Cluster không thay đổi, store không cộng đôi.
4. Trên fixture riêng, thêm batch có dòng trùng và hóa đơn giao nhau; đối chiếu với
   full recompute, kiểm tra output ngày khác không bị ghi đè.
5. Training version mới, thử save/load và xác nhận daily không fit lại model.
6. Chạy DAG test với ngày dữ liệu tường minh, kiểm tra cả downstream/publish và log retry.

Trạng thái xác minh khi viết guide: AST của backup, daily và training đã qua;
test CLI trên interpreter host trả `1 skipped` vì thiếu PySpark. Các lần kiểm tra
trước ghi nhận giới hạn Hadoop/winutils trên Windows. Chưa chạy các lệnh Docker
trong guide này, chưa xác nhận real-disk end-to-end hoặc DAG tích hợp đã thành công.
Không dùng kết quả skip làm bằng chứng pass.
