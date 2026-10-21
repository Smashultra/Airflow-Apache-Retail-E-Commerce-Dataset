> Publication update 2026-10-21: user authorized committing and pushing this review to `codex/rfm-supervised-20261019`. Earlier uncommitted/no-push statements below record verification checkpoints. No deployment or model activation is implied.

# RFM supervised — bản sửa review ngày 21/10/2026

Classifier học pseudo-label của KMeans để daily dự đoán trực tiếp mã Cluster.
Không đặt tên nghiệp vụ cho cụm, không có VIP hoặc quy tắc giữ nhãn.

## Training, chọn tham số và refit

1. Đóng băng KMeans teacher/scaler tại train cutoff. CustomerID chỉ dùng xác định cohort, không là feature.
2. Fit mỗi RandomForest trên RFM training; grid mặc định numTrees=[60,150], maxDepth=[4,8,12].
3. Chấm validation và tách all/seen/new. New là CustomerID chưa có trong snapshot train.
4. Chọn cấu hình theo validation.new.macro_f1. Nếu bằng nhau, ưu tiên ít cây hơn rồi độ sâu thấp hơn.
5. Gate chỉ dùng macro-F1 validation của khách mới. Không có khách mới thì fail, không tự chuyển sang đánh giá khách cũ.
6. Chấm test đúng một lần bằng model đã chọn. Không dùng test để chọn tham số hoặc gate release.
7. Refit student trên snapshot mới nhất (refit cutoff >= test cutoff), giữ teacher/scaler cố định để bảo toàn ý nghĩa ID.
8. Lưu model triển khai và metadata sau cùng. Daily cutoff phải sau fit_cutoff, không phải chờ thêm hai tháng sau khi model mới đã fit.

**Test đánh giá model trước refit.** Metadata lưu evaluated_model_id riêng với model_id của model triển khai, test_scope=pre_refit_model_only. Không gán kết quả holdout cho model refit như thể model đó đã được kiểm chứng trên dữ liệu chưa thấy. Theo dõi model triển khai trên các ngày sau cutoff.

Không lấy số 150/8 từ ảnh review làm cấu hình cố định. Code chọn lại từ dữ liệu validation của từng lần chạy; các con số teammate cung cấp chưa được tái lập trong lần sửa này.

## Metrics

Mỗi cohort có số khách, agreement, confusion matrix, precision/recall/F1 từng lớp và macro-F1.
Macro-F1 tính trên hợp của các lớp xuất hiện ở nhãn thật hoặc dự đoán. Lớp hoàn toàn vắng mặt được ghi F1=null và loại khỏi mẫu số; lớp chỉ bị dự đoán nhầm vẫn có F1=0 và bị tính lỗi.
Cohort rỗng ghi metrics=null, không giả định 0 hoặc 1. Báo cáo có macro_labels và support để thấy coverage.
Agreement/F1 là mức khớp teacher, không phải chất lượng nhãn nghiệp vụ hay dự đoán churn.

## Daily và model contract

Daily: giao dịch mới → state/RFM → preprocessing đã lưu → RandomForest → Cluster.
Không fit lại model. confidence là điểm chưa hiệu chuẩn và không điều khiển giữ/đổi nhóm.
Loader kiểm tra schema_version=3, ngày, số lớp, model ID, thứ tự/loại stage, các cột input/output,
scaler hữu hạn, chiều feature và hyperparameter khớp metadata. Metadata cần marker _SUCCESS.
Bundle v2 cũ không được nạp bằng contract mới: train phiên bản mới để có provenance đầy đủ.

CLI ghi partition run_date=D nhưng không lặp cột run_date trong file Parquet, giống nhánh KMeans.
Retry dựng lại run_date từ cutoff để so sánh: cùng nội dung tái dùng, khác nội dung hoặc output thiếu _SUCCESS bị từ chối.

## Airflow đã được nối trong code

- retail_rfm_training_dag mặc định model_kind=classifier; có thể chọn kmeans để dùng luồng cũ.
- build_training_context đóng băng grid và ngày teacher/validation/test. Nếu không chỉ định, dùng training_cutoff trừ 90/60/30 ngày; production refit dùng chính training_cutoff.
- select_kmeans_k dùng teacher cutoff cho báo cáo chọn k, không dùng dữ liệu validation/test. k vẫn do người dùng quyết định.
- retail_training tạo teacher, chọn/refit classifier, lưu/đọc lại bundle rồi ghi candidate theo model registry hiện có.
- Registry kiểm tra hash của toàn bundle; publish_model giữ cơ chế kích hoạt hiện tại và fail không thay serving model.
- retail_pipeline.run_rfm đọc model_kind từ manifest đã pin. Với classifier, load_bundle và score; với model cũ, dùng KMeans. Không đổi model giữa retry.
- Đã bỏ rfm_local_workflow.patch lỗi thời. Không cần áp dụng patch thủ công nữa.

Đây là **tích hợp ở mức code và test dispatch**, chưa là bằng chứng scheduler đã chạy thành công. Model KMeans đang active vẫn được dùng cho tới khi classifier mới được train và activate. Những tháng đầu chưa đủ lịch sử/khách mới có thể fail; cần chọn cutoff phù hợp, không nới validation để chạy cho qua.

Airflow sử dụng các lát event-time của ETL bất biến tại cutoff hiện tại. Đây là đánh giá retrospective; nếu dữ liệu có sửa hồi tố hoặc đến muộn thì cần archive theo thời điểm đã biết để tái hiện online chính xác. Không tuyên bố lọc InvoiceDate tự giải quyết availability leakage.

## Chạy CLI độc lập

```sh
spark-submit scripts/train_rfm_classifier.py --store-path data/curated/rfm_store --teacher-path data/models/kmeans/v1 --train-date 2011-08-01 --validation-date 2011-09-01 --test-date 2011-10-01 --refit-date 2011-11-01 --num-trees-grid 60 150 --max-depth-grid 4 8 12 --model-output data/models/classifier/v3

spark-submit scripts/pyspark_rfm.py --input data/curated/batch --batch-id batch1 --run-date 2011-11-02 --store-path data/curated/rfm_store --model-kind classifier --model-path data/models/classifier/v3 --model-version classifier_v3 --output data/analytics/rfm_classifier_v3
```

Ngày trên là ví dụ. CLI yêu cầu store có snapshot đúng từng cutoff và teacher_info khớp snapshot train. Không lấy snapshot tương lai để suy ngược quá khứ. Ghi riêng đường dẫn mới, không ghi đè bundle đã công bố.

Các file dùng chung: retail_contracts.py (contracts và metrics/validation thuần Python), rfm_supervised.py (Spark fit/evaluate/load/score). train_rfm_classifier.py điều phối CLI, retail_training.py điều phối training qua registry.

API tham khảo: [Spark RandomForestClassifier](https://spark.apache.org/docs/3.5.6/api/python/reference/api/pyspark.ml.classification.RandomForestClassifier.html).


---

# Verification - peer review correction 2026-10-21

Checkout: rfm_push_20261019, branch codex/rfm-supervised-20261019, base 518a976.
Uncommitted changes only; no new push, model activation or production data run.

## Reproduced checks

Runtime: Windows, Python 3.9.25 (existing spark_env), PySpark 3.5.9, Java 17.
pytest/numpy/pandas/pyarrow installed in workspace-only rfm_review_deps to support testing;
no project requirements changed. Tests use normal imports, not AST extraction.

1. `pytest -q tests/test_supervised_contracts.py tests/test_supervised_rfm.py tests/test_retail_models.py`
   at the first verification point: **17 passed, 3 skipped, 59.57s**.
   Covers exact metrics, selection/gate, real Spark classification/schema/cohorts,
   invalid pipeline, CLI provenance failures, Airflow adapter classifier dispatch,
   registry context/hash checks and existing activation failure tests.
2. After adding the real grid/refit regression:
   `pytest -q tests/test_supervised_rfm.py -k real_temporal_grid`
   **1 passed, 11 deselected, 76.64s**. Two actual RF candidates, validation on new IDs,
   one selected-model test evaluation and production refit with a distinct model UID.
   Synthetic data: this is functional verification, not a quality benchmark.
3. `pytest -q tests/test_retail_contracts.py tests/test_pyspark_rfm.py -k 'context or cli'`
   **17 passed, 107 deselected, 1.53s**.
4. `pytest -q tests/test_supervised_lifecycle.py`: **1 skipped, 0.65s**.
5. Python AST syntax checks: **27 files passed**. Git diff whitespace check passed.

Across these non-overlapping tests: **35 passed, 4 skipped**.
The three persistence/CLI-output tests and one full lifecycle test are explicitly
skipped on Windows without HADOOP_HOME/native filesystem support. They must run in
project Linux/Docker; skipped is NOT passed.

## Remaining verification

- Bundle persistence roundtrip, cutoff rejection after disk load, immutable output
  retry/schema and complete standalone daily CLI.
- Full bootstrap -> classifier training -> registry activation -> daily lifecycle.
- Actual Airflow DagBag import/serialization/scheduler run with the project's versions.
- Full-data tuning, performance and quality/coverage by cohort. No winning full-data
  parameters or score from the teammate's screenshot were reproduced here.
- Replay with point-in-time availability if source records can arrive late/be revised.

Suggested project Docker command after mounting this checkout:

```sh
python -m pytest -q tests/test_supervised_contracts.py tests/test_supervised_rfm.py tests/test_supervised_lifecycle.py tests/test_retail_models.py tests/test_retail_training.py tests/test_retail_dag.py
```

## Scope of evidence

Teammate reported 4 new-test passes, 341 existing-test passes and a validation grid
on the prior code. Those are external review evidence, not results reproduced above.
Earlier local evidence from 16/19 October does not certify these new changes.

Test metrics apply to the selected pre-refit model. Production refit has its own ID
and fit cutoff; the code does not claim holdout quality for that refitted model.
Teacher/scaler remain frozen so refit does not silently rename numeric clusters.
Actual daily runs use classifier only after a valid classifier bundle is activated;
an existing pinned KMeans model continues to use the backward-compatible KMeans path.


---

# Đối chiếu review RFM

Ảnh review là nguồn nhận xét và số liệu teammate báo cáo. Không coi 341 test pass hoặc bảng agreement trong ảnh là kết quả tự chạy lại.

| Nhận xét | Cách xử lý | Bằng chứng/giới hạn |
|---|---|---|
| RF gán cứng 60/8 | Grid có CLI và DAG params; chọn validation.new.macro_f1, lưu candidates/selected_params | Grid/refit Spark thật trên dữ liệu nhỏ đạt |
| Validation chỉ dùng gate | Dùng để chọn tham số và gate; tie-break xác định trước | Test chọn theo khách mới, không theo all |
| Chưa vào Airflow | Nối monthly trainer, model registry và daily dispatch; bỏ patch cũ | Test dispatch Spark đạt; DagBag/scheduler và lifecycle disk còn cần Docker |
| Đánh giá thiên khách cũ | Báo cáo all/seen/new, new theo ID chưa xuất hiện trong train | Test cohort đạt; chưa benchmark lại dữ liệu thật |
| Macro-F1 phạt lớp hoàn toàn vắng | Macro trên union nhãn/dự đoán, báo coverage; predicted-only vẫn bị phạt | Test confusion tính tay đạt |
| Model cũ khi deploy | Refit student tại latest cutoff, teacher giữ cố định; tách evaluated_model_id/model_id | Test thứ tự và refit thật đạt; metric test không đại diện model refit |
| Test bị dùng làm gate | Test chỉ báo cáo một lần sau selection, trước refit | Test điểm test=0 vẫn không thay quyết định validation |
| run_date lặp trong Parquet | Bỏ cột vật lý; dựng lại cutoff khi so sánh retry | Test persistence bổ sung, skip trên Windows thiếu Hadoop native |
| Loader chưa kiểm pipeline | Kiểm stage/type/column/scaler/dimensions/classes/hyperparameters/provenance/marker | Test cấu trúc pipeline thật đạt; roundtrip disk còn cần Docker |
| Test tự so implementation/AST | Bỏ AST; import module bình thường; test nhãn ở hai cực, schema, metrics, cohorts, registry và orchestration | Không dùng test fit/score equality làm bằng chứng duy nhất |

Các thay đổi nằm trong checkout review rfm_push_20261019, nhánh codex/rfm-supervised-20261019.
Chưa commit/push. Không chép đè workflow cũ đang có thay đổi riêng trong checkout initial-setup.


## Tổ chức sau khi gộp file

- `pyspark_rfm.py`: tính RFM và chạy daily CLI.
- `rfm_supervised.py`: fit, chọn classifier, đánh giá, refit, lưu/nạp và predict.
- `train_rfm_classifier.py`: entry point CLI training.
- `retail_contracts.py`: kiểm tra và metrics thuần Python, không import Spark.
- `test_supervised_contracts.py`: metrics, tham số, CLI và registry.
- `test_supervised_rfm.py`: kiểm thử Spark; `test_supervised_lifecycle.py`: vòng đời tích hợp.

Các số liệu kiểm thử phía trên được ghi trước khi gộp file. Sau khi gộp, chạy lại `pytest -q tests/test_supervised_contracts.py tests/test_retail_models.py tests/test_retail_contracts.py`: **20 passed (5.28s)**. Các kiểm tra Docker/Airflow còn thiếu vẫn giữ nguyên trạng thái.
