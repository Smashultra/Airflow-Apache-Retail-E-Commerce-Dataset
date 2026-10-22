# RFM classifier — bàn giao cho người chạy DAG

Cập nhật 22/10/2026. Tài liệu này mô tả code hiện tại để tích hợp lên `main`.
Không cần áp dụng patch thủ công. Chạy các lệnh từ repo được Docker Compose mount,
không dùng một checkout khác có đường dẫn `data/` khác.

## 1. Phần RFM đã làm gì?

- KMeans là teacher tạo pseudo-label số `Cluster`; RandomForest học các nhãn đó.
- Training thử grid RF, chọn theo macro-F1 khách mới trên validation, chấm test,
  rồi refit RF trên snapshot mới nhất với teacher/scaler cố định.
- Daily nạp model đã được registry gắn với lần chạy và predict; không fit lại.
- Kiểm tra ngày, snapshot, stages, metadata và hash bundle; retry không tự đổi model.
- Đã bỏ agreement khỏi báo cáo mới. Không gán tên VIP hay giữ nhãn theo số ngày.

Đây là học cách phân nhóm của KMeans, không phải dự đoán mua lại/churn tương lai.
Cluster có thể đổi khi RFM đổi. Số cluster giữa hai phiên bản teacher khác nhau
chưa được căn chỉnh ý nghĩa. Điểm test chỉ thuộc model trước refit.

## 2. Bản đồ file và đường gọi

| File | Trách nhiệm |
|---|---|
| `scripts/pyspark_rfm.py` | Tính RFM, điểm R/F/M, log features, KMeans helpers, kiểm tra output; entry point daily |
| `scripts/rfm_supervised.py` | Fit/evaluate/select/refit RF; save/load bundle; predict Cluster |
| `scripts/retail_contracts.py` | Context/hash/path; kiểm tra ngày/grid; metrics và quy tắc xếp hạng RF dùng chung |
| `scripts/retail_models.py` | Training context, registry, model binding, hash và activation |
| `scripts/retail_training.py` | Spark job training qua registry; bootstrap ETL không cần model |
| `scripts/retail_pipeline.py` | Daily từ ETL snapshot → RFM → model → customers/cluster_profile |
| `scripts/select_kmeans_k.py` | Báo cáo hỗ trợ chọn K tại cutoff teacher; không tự chọn K |
| `dags/retail_rfm_training_dag.py` | Validate → evaluate K → report-only gate → train → publish |
| `dags/ecommerce_etl_dag.py`, `dags/ecommerce_etl_dag_allinone.py` | ETL trước nhánh RFM/anomaly; công bố kết quả sau thành công |
| `dags/retail_support/` | Operator Spark, context, publication, assets và lineage |
| `scripts/retail_spark_lineage.py` | Dependency của Spark operator/lineage hiện có |

```text
Training DAG → retail_training.py → rfm_supervised.py → candidate → publish_model
Daily DAG → pyspark_rfm.py --pipeline-context ...
          → retail_pipeline.run_rfm() → load_bundle()/score()
```

`train_rfm_classifier.py` và `train_rfm_kmeans.py` là CLI độc lập, không được DAG
training gọi. CLI dùng incremental store; DAG hiện tính RFM từ lịch sử ETL tích lũy.
Không mô tả daily DAG là đã dùng incremental customer-state store.

## 3. Ngày, features và đánh giá

Với `training_cutoff=2011-11-01`, mặc định:

| Mốc | Cutoff exclusive | Công việc |
|---|---|---|
| Teacher/train | 2011-08-03 (T−90 ngày) | Fit scaler + KMeans; fit các RF thử nghiệm |
| Validation | 2011-09-02 (T−60 ngày) | Chọn/gate RF theo khách mới |
| Test | 2011-10-02 (T−30 ngày) | Chấm model đã chọn một lần |
| Refit | 2011-11-01 (T) | Fit RF mới bằng cấu hình thắng, giữ teacher/scaler |

Snapshot dùng giao dịch có `InvoiceDate < cutoff`. R = ngày chốt trừ ngày mua cuối;
F = số hóa đơn khác nhau; M = tổng Quantity × UnitPrice. Features là log1p(R,F,M)
qua assembler/scaler học ở train. CustomerID và RFM_score không làm features.

Snapshot tích lũy, không phải bốn tập giao dịch rời nhau. New/seen đều so với ID
ở train; khách mới ở validation có thể tiếp tục thuộc cohort new ở test.
Đây là replay theo event-time; nếu dữ liệu đến muộn/sửa hồi tố, cần dữ liệu lưu
theo thời điểm đã biết để đánh giá availability leakage.

Grid mặc định: numTrees=[60,150], maxDepth=[4,8,12]. Hòa điểm: ít cây trước,
độ sâu nhỏ hơn sau. Gate mặc định 0.8; thiếu khách mới validation thì fail,
không thay bằng điểm all/seen. Test không tham gia chọn/gate.
Macro-F1 tính trên hợp các lớp có nhãn hoặc dự đoán; lớp hoàn toàn vắng được bỏ.
Giữ số khách, confusion và per-class metrics để kiểm tra coverage.

Metadata tách `evaluated_model_id`, `model_id`, `fit_cutoff`, `teacher_cutoff` và
`test_scope=pre_refit_model_only`. Bundle schema v3; bundle cũ cần train lại.
`confidence` chỉ tham khảo, chưa hiệu chuẩn và không giữ/đổi nhãn.

## 4. Chuẩn bị môi trường Airflow

Kiểm tra checkout mới đã được mount vào container. Nếu container cũ chưa nhận
PYTHONPATH, khi không có job đang chạy, tạo lại riêng service Airflow:

```sh
docker compose ps
docker compose exec -T airflow-scheduler printenv PYTHONPATH
docker compose up -d --no-deps --force-recreate airflow-apiserver airflow-scheduler airflow-dag-processor
docker compose exec -T airflow-scheduler python -c "import retail_contracts, retail_models, rfm_supervised; print(retail_contracts.__file__)"
docker compose exec -T airflow-dag-processor airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow pools set retail_spark 1 "One local Spark JVM"
```

Compose đã có PYTHONPATH chứa `/opt/airflow/scripts` và `/opt/airflow/dags`.
Không xóa volume PostgreSQL. Đây là hướng dẫn vận hành; chưa tuyên bố scheduler
UI đã được kiểm chứng chỉ vì standalone hoặc unit test pass.

## 5. Chạy training DAG rồi daily

Chuẩn bị `data/raw/data.csv`, sau đó tạo landing và ETL bootstrap:

```sh
docker compose exec -T airflow-scheduler python scripts/prepare_daily_landing.py
docker compose exec -T airflow-scheduler spark-submit --master "local[2]" --driver-memory 3g scripts/retail_training.py --bootstrap-date 2011-10-31 --run-id bootstrap_rfm_20111101
```

Bootstrap ngày 31/10 tạo ETL cutoff 01/11. Không chạy lệnh Spark thủ công song song
với DAG Spark: lệnh này không đi qua pool Airflow.

Trigger `retail_rfm_training_dag` với Params:

```json
{
  "model_kind": "classifier",
  "training_cutoff": "2011-11-01",
  "effective_from": "2011-11-02",
  "source_version": "online-retail-v1",
  "etl_manifest": null,
  "rfm_k": 2,
  "report_only": false,
  "num_trees_grid": [60, 150],
  "max_depth_grid": [4, 8, 12],
  "min_macro_f1": 0.8
}
```

Giữ `classifier_*_date=null` để dùng các mốc mặc định ở trên. K=2 là cấu hình demo;
nếu cần chọn K, chạy `report_only=true` trước rồi tạo DagRun mới với K đã chọn.
Xem [hướng dẫn chọn K](K_SELECTION_VI.md). Báo cáo ghi riêng `training_cutoff`
(ETL/refit) và `teacher_cutoff`; không nhập ngày teacher vào training_cutoff.

Training DAG có giới hạn logical date lịch sử tới hết 01/11/2011. Dùng một logical
date lịch sử chưa có, ví dụ `2011-11-01T00:00:00Z`; logical date và cutoff là hai
khái niệm khác nhau. Không dùng ngày hiện tại hoặc tái dùng logical date đã tồn tại.

Đợi 5 task thành công, kiểm tra log `MODEL_ACTIVATED`, registry
`data/manifests/models/registry.json`, model manifest và bundle tại
`data/models/rfm/version=<id>/`. Thất bại training không thay model đang phục vụ.

Sau đó trigger daily DAG với:

```json
{
  "business_date": "2011-11-01",
  "source_version": "online-retail-v1",
  "model_version": null,
  "rfm_k": null,
  "churn_days": 90
}
```

Business date 01/11 nghĩa là cutoff 02/11, hợp lệ sau refit 01/11.
Chọn logical date thuộc khoảng lịch sử của daily DAG. Kiểm tra manifest model
được pin có `model_kind=classifier`. Output `customers` phải có Cluster,
model_version và supervised_model_id; `cluster_profile` chứa thống kê từng cụm.
Đọc đường dẫn generation từ manifest, không đoán thư mục output mới nhất.
`segment`/inactivity là quy tắc nghiệp vụ riêng, không phải tên cluster.

Retry cùng DagRun phải giữ model binding. Kiểm tra publication thành công trước
khi cho downstream sử dụng. Pause DAG sau demo lịch sử nếu không cần chạy tiếp.

## 6. Kiểm thử và bằng chứng

```sh
docker compose exec -T airflow-scheduler python -m pytest -q -p no:cacheprovider tests/test_supervised_contracts.py tests/test_supervised_rfm.py tests/test_supervised_lifecycle.py tests/test_k_selection_context.py tests/test_retail_models.py tests/test_retail_training.py tests/test_retail_dag.py tests/test_rfm_publication.py
```

- Contract tests: metrics, selection, dates, CLI và registry/hash.
- Spark tests: cohorts, real grid/refit, provenance/gate, bundle persistence,
  output retry, daily dispatch và metadata mismatch.
- Lifecycle: bootstrap → train → activate → daily qua code registry, không phải
  một DagRun thực thi bởi scheduler.
- K report regression: dùng ngày teacher để tính features nhưng giữ ngày ETL/refit
  trong trường training_cutoff; legacy KMeans giữ hai ngày bằng nhau.

Đã tự chạy standalone trên dataset thật, Docker Spark 4.2.0, 6 bước đều exit 0:
prepare → teacher CLI → classifier CLI → daily → daily retry → verify.
Store thử dùng một bucket; không phải benchmark hiệu năng production.

| Kết quả lần standalone | Giá trị |
|---|---|
| Khách train/validation/test/refit | 3165 / 3321 / 3613 / 3970 |
| RF được chọn | 150 cây, depth 8 |
| Validation new | 156 khách; macro-F1 0.959540 |
| Test new, trước refit | 448 khách; macro-F1 0.949182 |
| Daily cutoff 02/11 | 3981 khách duy nhất; cluster 0=2421, cluster 1=1560 |
| Retry cùng batch/ngày | Thành công, exact-content check đạt |
| Tổng 6 bước | 274.40 giây, không tính khởi động/sửa môi trường |

Kết quả này khác lần teammate báo cáo (60/8, test 0.9408); không coi hai lần chạy
là cùng model/partitioning. Teammate báo 25 test RFM/registry pass, 0 skip và full
suite 703 pass/2 fail (anomaly POST, thiếu mount .agent); đó là bằng chứng ngoài,
không phải full-suite pass do tác giả tự tái hiện. Các test Windows trước đây dùng
Spark 3.5.9, không thay thế kiểm chứng phiên bản dự án 4.2.0.

Còn cần người làm DAG xác nhận live UI/scheduler trên môi trường triển khai,
nguồn dữ liệu/registry thực tế và downstream. Không cần viết lại classifier.
Validation ít khách mới nên báo rõ độ hạn chế khi so sánh cấu hình; không điều
chỉnh split dựa trên điểm test để tìm kết quả đẹp hơn.

### Pre-main verification ? 22/10/2026

The eight-file Docker test command above completed: **47 passed, 0 skipped, 1 warning**, 290.20 seconds. Warning: PySpark testing utilities report incomplete pandas >=3 compatibility in the image. All selected tests passed; this is not a full-project suite result or a live scheduler DagRun.
