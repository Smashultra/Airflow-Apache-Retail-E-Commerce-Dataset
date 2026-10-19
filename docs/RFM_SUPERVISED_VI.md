# RFM classifier: dự đoán trực tiếp cluster

Bản review ngày 19/10/2026. Cluster chưa được đặt tên nghiệp vụ.

## Luồng xử lý

- Training: RFM tại train cutoff → KMeans đã fit tạo nhãn Cluster → RandomForest học nhãn.
- Daily: batch mới → cập nhật customer state → tính RFM → RandomForest dự đoán Cluster.
- Daily classifier không tính khoảng cách đến tâm KMeans và không fit lại model.
- Cluster là mã số 0..k-1, không phải thứ hạng hay tên nhóm khách hàng.
- Không giữ nhãn qua nhiều ngày và không cần output phân nhóm ngày trước. Kho customer state vẫn cần cho RFM tích lũy; đây là trạng thái giao dịch, không phải trạng thái nhãn.

## Các file chính

| File | Chức năng |
|---|---|
| scripts/pyspark_rfm.py | Incremental ingest, tính RFM, chọn model, scoring và ghi output |
| scripts/train_rfm_kmeans.py | Training teacher KMeans; file có sẵn |
| scripts/train_rfm_classifier.py | Training RandomForest, đánh giá theo thời gian, lưu bundle |
| scripts/rfm_supervised.py | Các hàm dùng chung: fit_student, evaluate, load_bundle, score, daily |
| tests/test_supervised_contracts.py | Kiểm thử contract không cần Spark |
| tests/test_supervised_rfm.py | Kiểm thử Spark: Cluster bằng prediction trực tiếp |

## Model và output

RandomForest dùng 60 cây, maxDepth=8, seed=42. Đây là cấu hình khởi đầu, chưa tune.
R/F/M được log1p và dùng assembler/scaler đã fit của teacher. CustomerID không là feature.
Hàm score ghi prediction cast int trực tiếp vào Cluster. confidence chỉ là điểm model chưa hiệu chuẩn, không quyết định giữ/đổi nhóm.
model_version và supervised_model_id phục vụ truy vết. Không tạo tên nhóm, RawCluster, is_vip hoặc candidate_days.

Bundle mới có schema_version=2; từ chối bundle chính sách cũ. Train ra đường dẫn mới.
ID cluster giữa hai phiên bản KMeans có thể mang ý nghĩa khác nhau; không tự suy ra ánh xạ.

## Đánh giá và chống leakage

Yêu cầu train < validation < test, snapshot store đúng cutoff. Teacher và student dùng cùng snapshot training.
Chỉ fit ở train; validation/test chỉ transform. Daily cutoff phải sau test cutoff.
Không dùng snapshot cuối kỳ để suy ngược quá khứ. Dữ liệu đến muộn/sửa hồi tố phải được quản lý theo thời điểm đã biết.
Agreement, precision/recall và macro-F1 đo mức bắt chước KMeans, không phải chất lượng nhãn nghiệp vụ thật hay dự đoán mua lại/churn.
Không tune theo test. Gate macro-F1 mặc định 0.8 chưa được xác nhận bằng dữ liệu thật.

## Ví dụ lệnh

Thay ngày và đường dẫn bằng snapshot thực tế đã kiểm tra:

```sh
spark-submit scripts/train_rfm_classifier.py --store-path data/curated/rfm_store --teacher-path data/models/kmeans/v1 --train-date 2011-09-01 --validation-date 2011-10-01 --test-date 2011-11-01 --model-output data/models/classifier/v2

spark-submit scripts/pyspark_rfm.py --input data/curated/batch_2011_11_01 --batch-id batch_2011_11_01 --run-date 2011-11-02 --store-path data/curated/rfm_store --model-kind classifier --model-path data/models/classifier/v2 --model-version classifier_v2 --output data/analytics/rfm_classifier_v2
```

Không cần previous-output, bootstrap-segments hay tên cluster nghiệp vụ.
initialize-store là cờ khởi tạo kho giao dịch, không phải khởi tạo nhãn.
Classifier output giữ bất biến: retry cùng kết quả tái sử dụng; khác kết quả tại đường dẫn đã tồn tại bị từ chối.
Mặc định model-kind vẫn là kmeans để giữ tương thích.

## Phạm vi nhánh review

Nhánh codex/rfm-supervised-20261019 chứa bản sửa dự đoán trực tiếp, thay bản d986dcf đã thu hồi.
Các file workflow dùng chung trên main được giữ nguyên. docs/patches/rfm_local_workflow.patch lưu thay đổi adapter/DAG local để review, chưa áp dụng; không áp dụng tự động lên model registry mới.
Chưa chạy training thật hoặc daily Airflow cho bản này. Xem RFM_SUPERVISED_VERIFICATION.md.
