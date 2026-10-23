# Chọn K trong DAG training

Luồng: `validate_training_input → evaluate_k → continue_training → train_rfm_model → publish_model`.

`evaluate_k` thử K từ 2 đến `k_max` (mặc định 8, giảm theo số khách),
dùng đúng snapshot ETL và seed trong context training. Với classifier, cutoff
đánh giá K là `classifier_train_date` (ngày teacher), không phải cutoff refit.
Dữ liệu là
lịch sử tích lũy trước cutoff, không chỉ tháng gần nhất. Task chạy Spark trong
pool `retail_spark`, không tự chọn K hay thay đổi `rfm_k`.

## Xem báo cáo trước khi quyết định

1. Chuẩn bị snapshot ETL đúng cutoff như luồng training hiện tại.
2. Trigger `retail_rfm_training_dag` với `training_cutoff`, `report_only=true`
   và `k_max` mong muốn. `rfm_k` không được dùng để train trong chế độ này.
   Đặt logical date lịch sử, ví dụ `2011-10-31T00:00:00Z`; không dùng ngày
   hiện tại vì task bị giới hạn end_date `2011-11-01T23:59:59.999999Z`.
3. Xem log task `evaluate_k`: mỗi K có WSSSE và silhouette, cuối log có đường
   dẫn báo cáo. Trên máy host mở `data/analytics/k_selection/<run_key>/`:
   `k_scores.csv`, `report.json`, và biểu đồ PNG nếu có matplotlib.
4. Chọn K bằng tay. Trigger **DagRun mới** với `report_only=false`, `rfm_k`
   đã chọn, cùng `training_cutoff` và `etl_manifest` ghi trong `report.json`.
   Giữ cùng `source_version`. DAG tính báo cáo lại rồi train và publish.
   `training_cutoff` trong báo cáo là cutoff ETL/refit; `teacher_cutoff` là
   ngày dùng để chọn K. Không nhập `teacher_cutoff` vào Param `training_cutoff`.
   Nếu đã tùy chỉnh `classifier_train_date`, giữ nguyên giá trị này ở lần train.
   Với báo cáo cũ chỉ có một trường ngày, hãy tạo lại báo cáo bằng code mới.
   Run train có thể dùng logical date `2011-11-01T00:00:00Z`. Hai ví dụ trên
   đều dùng cutoff `2011-12-01`; logical date không thay cutoff của manual run.
   Nếu logical date đã được dùng, chọn mốc lịch sử khác chưa dùng trong giới hạn
   DAG; không chỉ đổi run_id rồi giữ logical date trùng.

Ví dụ cutoff `2011-06-01` dùng các giao dịch trước ngày 01/06, tức đến hết
31/05. Phải có snapshot ETL của cutoff này; task không tự tạo dữ liệu còn thiếu.

Mặc định `report_only=false`: chạy báo cáo rồi train ngay với `rfm_k` (mặc
định vẫn là 2), không dừng chờ người dùng. Chạy lịch tháng cũng theo hành vi
này. Không sửa Params của DagRun đã chạy để đổi K; tạo DagRun mới.

Daily RFM tiếp tục dùng model đã publish và K của model đó. Báo cáo không làm
thay đổi model đang phục vụ. Bước thử K làm tăng thời gian chạy; lỗi báo cáo
sẽ chặn các task train/publish phía sau. Dữ liệu quá ít có thể không đủ để
tính silhouette; xem lỗi task và điều chỉnh dữ liệu/khoảng K.
