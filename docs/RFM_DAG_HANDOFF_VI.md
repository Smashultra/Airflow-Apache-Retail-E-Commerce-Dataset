# Bàn giao RFM cho người làm DAG

Cập nhật: 13/10/2026. Source đã push lên `main` tại commit `40bbca2`.

## 1. Mục tiêu và trạng thái bàn giao

Daily tiếp nhận batch curated mới, cập nhật incremental store, rồi scoring **toàn bộ khách hàng có lịch sử mua hợp lệ trong store** bằng model KMeans đã lưu. Khách không mua trong ngày vẫn xuất hiện trong output. Training KMeans là job riêng.

| File | Trách nhiệm |
| --- | --- |
| `scripts/pyspark_rfm.py` | Ingest incremental, tính RFM, load model, daily scoring và ghi output |
| `scripts/train_rfm_kmeans.py` | Fit và lưu một phiên bản model mới; dùng chung helper RFM |
| `tests/test_pyspark_rfm.py` | Kiểm tra dữ liệu, tính RFM, CLI, model và incremental store |

Dùng các file trong `scripts/` và `tests/`, không dùng file trong `.agent/backups/`.

**Điểm cần nối khi làm DAG:** luồng CLI standalone của `pyspark_rfm.py` đã chạy incremental và all customers. Khi truyền `--pipeline-context`, script chuyển sang `retail_pipeline.pipeline_main("rfm")`. Tại bản upstream làm nền cho commit này (`5817dfb`), adapter đó vẫn đọc lịch sử curated và tính lại RFM bằng `compute_rfm`, chưa dùng incremental store. Các thay đổi adapter/DAG ở checkout local không được đưa vào commit RFM.

Người làm DAG cần chọn một trong hai cách:

1. Gọi CLI standalone bằng task submit Spark, sau đó bổ sung publication/output theo hợp đồng DAG của nhóm.
2. Cập nhật adapter `retail_pipeline.run_rfm` để dùng incremental store, đồng thời giữ model registry, labels, cluster profile và publication manifests hiện có.

Không coi việc gọi `--pipeline-context` hiện tại là đã hoàn tất tích hợp incremental. Hai API `fit_rfm_model` và `apply_rfm_model` được giữ để tương thích với training/model registry của nhóm.

## 2. Luồng cần triển khai

```text
Bootstrap một lần:
Lịch sử curated đầy đủ -> khởi tạo store + training -> model v1

Daily:
Batch curated mới -> kiểm tra/đóng băng context -> cập nhật store
    -> RFM của toàn bộ khách trong snapshot -> transform model đã lưu
    -> kiểm tra output -> ghi partition ngày -> publish kết quả

Training định kỳ:
Snapshot store đủ dữ liệu -> train model mới -> kiểm tra -> công bố model
    -> áp dụng cho các run daily tiếp theo
```

Giữ xử lý Spark trong `scripts/`; DAG chỉ điều phối và submit job. Không đưa tính toán Spark vào `PythonOperator`.

## 3. Hợp đồng đầu vào và ngày chốt

Input là Parquet sạch, không rỗng, với schema tối thiểu:

| Cột | Spark type | Điều kiện |
| --- | --- | --- |
| `CustomerID` | string | Không null/rỗng |
| `InvoiceNo` | string | Không null/rỗng; đã loại giao dịch hủy |
| `InvoiceDate` | timestamp | Không null, trước cutoff |
| `Quantity` | int | > 0 |
| `UnitPrice` | double | > 0 và hữu hạn |

Giữ schema nghiệp vụ ổn định giữa các batch, gồm cả các cột bổ sung. Không đưa các cột dành riêng `batch_id`, `transaction_date`, `_customer_bucket` vào input. Cleaning chịu trách nhiệm áp dụng quy tắc loại giao dịch không hợp lệ của dự án.

`--run-date D` nghĩa là cutoff **00:00 UTC của D**, chỉ nhận giao dịch `InvoiceDate < D`. Ví dụ batch ngày `2011-12-10` được scoring tại cutoff `2011-12-11`. Input có giao dịch tại/sau cutoff bị từ chối.

DAG lấy cutoff từ khoảng dữ liệu của run và chuẩn hóa về UTC; không lấy ngày đồng hồ hiện tại cho retry/backfill. Chỉ cho downstream chạy khi upstream xác nhận dữ liệu trước cutoff đã đầy đủ.

## 4. Tham số của task daily

| Tham số | Cách truyền |
| --- | --- |
| `--input` | Bắt buộc: đường dẫn batch curated đã đóng băng |
| `--batch-id` | Bắt buộc: ID cố định cho cùng batch và cutoff |
| `--run-date` | Bắt buộc: cutoff `YYYY-MM-DD` |
| `--store-path` | Nên truyền rõ: store chung, bền vững, dùng xuyên các run |
| `--output` | Nên truyền rõ: output root |
| `--model-path` | Bắt buộc: thư mục PipelineModel đã được công bố |
| `--model-version` | Nên truyền rõ theo registry; mặc định lấy tên thư mục model |
| `--initialize-store` | Chỉ bootstrap store chưa có, bằng lịch sử đầy đủ |

Daily không nhận `--k`, `--seed`, `--all-customers`, `--period-start`, `--num-buckets`. Lấy k từ model; store mới mặc định 64 bucket, store cũ giữ cấu hình đã lưu.

Batch ID dài 1–128 ký tự, bắt đầu bằng chữ/số, chỉ chứa chữ/số, `_`, `-`. Ví dụ `source_v1_day_2011_12_10`; không gắn số attempt hoặc timestamp chạy lại.

Các đường dẫn input/store/output/model phải tách biệt. Store hiện hỗ trợ filesystem local hoặc HDFS với atomic rename; nếu chạy nhiều container, mọi task phải truy cập cùng vùng lưu trữ bền vững. Không dùng thư mục tạm riêng của worker.

## 5. Lệnh mẫu để đưa vào task submit Spark

Các lệnh dưới đây chạy **bên trong container**, với scripts tại `/opt/airflow/scripts` và data tại `/opt/airflow/data`. Đường dẫn và ngày là ví dụ; dữ liệu phải tồn tại và đúng cutoff. `k=4` chỉ là ví dụ, cần dùng k đã chọn bởi người phụ trách RFM.

### Bootstrap store và model đầu tiên

```bash
spark-submit --master 'local[2]' \
  --py-files /opt/airflow/scripts/pyspark_rfm.py \
  /opt/airflow/scripts/train_rfm_kmeans.py \
  --input /opt/airflow/data/curated/seed_before_2011_12_10.parquet \
  --store-path /opt/airflow/data/curated/rfm_store \
  --batch-id seed_before_2011_12_10 \
  --initialize-store --run-date 2011-12-10 \
  --k 4 --seed 42 \
  --model-output /opt/airflow/data/models/rfm_kmeans/v1
```

Chạy một lần với store/model path mới. Chỉ công bố model sau khi job hoàn tất cả PipelineModel và audit `rfm_training_info`. Nếu training thất bại sau khi store đã commit, lần chạy lại bỏ `--initialize-store`, giữ nguyên input/batch-id/cutoff; kiểm tra artifact model dở dang và dùng model-output mới nếu đường dẫn đã tồn tại.

### Daily sau bootstrap

```bash
spark-submit --master 'local[2]' \
  /opt/airflow/scripts/pyspark_rfm.py \
  --input /opt/airflow/data/curated/batch_2011_12_10.parquet \
  --store-path /opt/airflow/data/curated/rfm_store \
  --batch-id source_v1_day_2011_12_10 \
  --run-date 2011-12-11 \
  --model-path /opt/airflow/data/models/rfm_kmeans/v1 \
  --model-version v1 \
  --output /opt/airflow/data/analytics/rfm_daily
```

### Training định kỳ từ store

```bash
spark-submit --master 'local[2]' \
  --py-files /opt/airflow/scripts/pyspark_rfm.py \
  /opt/airflow/scripts/train_rfm_kmeans.py \
  --store-path /opt/airflow/data/curated/rfm_store \
  --run-date 2012-01-01 --k 4 --seed 42 \
  --model-output /opt/airflow/data/models/rfm_kmeans/v2
```

Không truyền input/batch-id khi training chỉ đọc store. Script chọn snapshot có cutoff không vượt ngày yêu cầu, tính Recency tại cutoff training rồi fit. DAG phải kiểm chứng độ đầy đủ của snapshot; script không biết dữ liệu upstream còn thiếu. Model-output phải là đường dẫn mới; không ghi đè phiên bản đã phát hành.

Với Spark chạy ngoài container submit, phân phối helper cho training qua `--py-files` và bảo đảm data/model/store truy cập được từ môi trường thực thi.

## 6. Output, retry và điều phối

Output standalone: `<output>/run_date=YYYY-MM-DD/`, gồm `CustomerID`, `LastPurchaseDate`, `Recency`, `Frequency`, `Monetary`, `R_score`, `F_score`, `M_score`, `RFM_score`, `Cluster`, `model_version`. `run_date` là cột partition; đọc từ output root hoặc dùng `basePath` để khôi phục cột này.

Frequency đếm hóa đơn riêng biệt theo khách; Monetary tích lũy và làm tròn khi xuất. Recency tính lại mỗi ngày. Điểm R/F/M tham chiếu toàn bộ population; model dùng log RFM với scaler/centroids đã fit. Daily không gọi fit. Cluster ID không tự mang nghĩa VIP/churn và không nhất thiết tương đương giữa hai phiên bản model.

Quy tắc retry:

- Giữ cố định input và nội dung, batch-id, cutoff, model-path, model-version trong mọi attempt của một run. Đóng băng model binding trước scoring, không đọc lại con trỏ “latest” mỗi lần retry.
- Cùng batch-id và nội dung/cutoff trả lại snapshot đã commit, không cộng đôi F/M. Cùng ID nhưng thay nội dung/cutoff bị lỗi.
- Store và output không commit chung: scoring có thể thất bại sau khi ingest thành công. Retry dùng snapshot cũ rồi scoring lại.
- Standalone ghi đè partition cùng ngày. Chỉ publish khi job thành công và output đã được xác minh; adapter dùng publication protocol của nhóm nếu cần output generations bất biến.
- Chỉ một writer cho mỗi store tại một thời điểm. Dùng Airflow pool riêng cho các task có ingest; giới hạn run để cập nhật theo cutoff tăng dần. Không tự xóa `_writer.lock` khi chưa xác nhận writer cũ đã dừng.
- Backfill chưa ingest với cutoff thấp hơn snapshot mới nhất bị từ chối. Chạy rebuild trên store riêng theo thứ tự ngày; retry batch đã commit có thể đọc lại đúng snapshot của batch đó.
- Training đọc snapshot đã commit; nếu chạy chế độ có input thì cũng phải dùng cùng pool writer. Kích hoạt model mới không thay binding của các run đang chạy.

## 7. Ngày không có giao dịch mới: cần xử lý trong adapter

CLI daily hiện bắt buộc input/batch-id và từ chối Parquet rỗng. Vì vậy chưa thể truyền batch rỗng để scoring ngày không có mua mới. Không bỏ qua scoring nếu yêu cầu là mỗi ngày đều có snapshot cho toàn bộ khách.

Người làm DAG cần thêm nhánh đọc store, không ingest khi batch hợp lệ nhưng rỗng. Dùng `load_manifest(spark, store_path, run_date=cutoff)` để lấy snapshot phù hợp, lưu version vào context của run, rồi gọi `daily_rfm_population(spark, manifest, cutoff)` và transform model đã đóng băng. Khi retry phải dùng đúng version đã lưu. Sau đó xác minh và publish theo cùng hợp đồng output. Phân biệt “batch rỗng hợp lệ” với “upstream thiếu/chưa hoàn tất dữ liệu”.

Nhánh này là công việc tích hợp DAG còn cần làm, chưa có trong CLI đã bàn giao. Cần test riêng cho ngày rỗng: đủ khách, F/M không đổi, Recency tăng theo cutoff, không fit model.

## 8. Tiêu chí nghiệm thu DAG

1. Bootstrap hoàn tất, có store và model hợp lệ; daily tiếp theo dùng đúng hai artifact đó.
2. Batch chỉ có khách A mua mới nhưng output vẫn có khách B không mua; B giữ F/M và cập nhật Recency.
3. Khách mới được thêm; batch chồng lặp/retry không cộng đôi; hóa đơn nhiều dòng vẫn đếm Frequency đúng.
4. Daily dùng model/scaler đã lưu, không fit; output có đúng model-version của run.
5. Retry sau ingest thành công nhưng ghi output thất bại trả kết quả nhất quán; không publish artifact dở dang.
6. Rerun một ngày giữ các partition ngày khác; backfill không phá store đang vận hành.
7. Kiểm tra nhánh ngày rỗng, model activation trong lúc retry, writer concurrency và đầy đủ dữ liệu tại cutoff.
8. Chạy trong Docker: ghi/đọc Parquet thật, lưu/load PipelineModel thật và test toàn luồng DAG.

Kiểm tra bộ RFM trong container đang có môi trường dự án:

```bash
python -m pytest -q /opt/airflow/tests/test_pyspark_rfm.py
```

Bằng chứng hiện có: 19 test RFM về CLI/daily và 11 test model/publication đạt trên host; syntax và diff checks đạt. Chưa chạy lại toàn bộ kiểm thử ghi Parquet/lưu model thật trong Docker vì daemon chưa bật. Kết quả này xác nhận phần core đã kiểm tra, chưa thay thế nghiệm thu end-to-end DAG.
