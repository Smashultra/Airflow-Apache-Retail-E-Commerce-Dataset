# Demo idempotency trên Airflow UI

## Bổ sung: Clear ETL với cùng đầu vào

Sau cập nhật ETL reuse, tạo DagRun mới (run tạo trước thay đổi code sẽ bị
kiểm tra code_version từ chối). Khi run thành công, có thể Clear riêng
`submit_pyspark_etl`, hoặc kèm Downstream để kiểm tra lại toàn luồng.
ETL kiểm tra context/code, checksum nguồn và các marker `_SUCCESS`, rồi log
`ETL_REUSED` và giữ nguyên etl.json/hash/output generation. Airflow vẫn gọi
spark-submit để khởi động script, nhưng script bỏ qua tạo SparkSession và
tính toán ETL. Các nhánh đã công bố tiếp tục dùng cùng ETL.
Nếu metadata/output/nguồn không hợp lệ, task thất bại thay vì ghi đè.
Không tự sửa các run đã bị lệch generation trước bản cập nhật; tạo run mới.

Mục tiêu: thực thi lại task đã công bố thành công trong cùng DagRun vẫn dùng
cùng snapshot, không submit Spark lại, không tạo thêm generation dữ liệu.
Chỉ bổ sung log `BRANCH_REUSED` vào cơ chế tái sử dụng có sẵn; không thêm Param/task.

## Thao tác ngắn nhất

1. Dùng run `ecommerce_etl_dag_allinone` đã phục hồi thành công trong
   [demo atomicity](DEMO_ATOMICITY_VI.md). Nếu dùng run khác, cần cả DAG thành công.
   Đợi DAG processor nạp code mới và giữ DAG unpaused.
2. Mở `detect_anomalies` → Logs, chọn attempt thành công. Lưu dòng
   `BRANCH_PUBLISHED audit`: chú ý `run_key`, `audit_hash`, `etl_hash`,
   `audit_manifest`. Đây là snapshot làm mốc so sánh.
3. Chọn **Clear** riêng `detect_anomalies` trong **run hiện tại**.
   Tắt Upstream, Downstream, Past, Future; kiểm tra preview chỉ có một task.
   Cho phép đưa DagRun trở lại running nếu UI hỏi. Không chọn Mark success.
4. Đợi task thực thi lại và thành công. Mở attempt mới trong Logs:

```text
BRANCH_REUSED stage=audit run_key=...; validated existing publication; Spark submission skipped; output generation unchanged
BRANCH_PUBLISHED audit {...}
```

5. So sánh hai attempt: `run_key`, `audit_hash`, `etl_hash`, `audit_manifest`
   phải giống nhau. Task có attempt mới nhưng snapshot vẫn là snapshot cũ.
   Thời gian thường ngắn hơn vì không chạy Spark, nhưng thời gian không phải
   bằng chứng chính. Dòng BRANCH_REUSED chỉ ghi sau khi publication được kiểm tra hợp lệ.

Không cần tắt `demo_fail_audit_once` nếu tiếp nối demo atomicity: khi publication
đã có, operator bỏ qua cả Spark lẫn điểm gây lỗi demo. Lần Clear để phục hồi
atomicity trước đó vẫn chạy Spark vì chưa có publication; lần Clear trong demo
này diễn ra SAU thành công nên tái sử dụng kết quả.

## Kết quả mong đợi

| Quan sát trên UI | Trước Clear | Sau Clear |
|---|---|---|
| detect_anomalies | Success | Thực thi lại rồi Success |
| Log BRANCH_REUSED | Không bắt buộc có | Có |
| audit_hash / etl_hash | Giá trị mốc | Giữ nguyên |
| Đường dẫn audit_manifest | Snapshot của run | Giữ nguyên |
| Các task khác | Success hoặc Skipped theo nhánh | Không được Clear |

Có thể làm tương tự với `compute_rfm_metrics`, đối chiếu `rfm_hash` và
`rfm_manifest`. Không cần chạy lại cả hai nhánh để trình bày ý chính.

## Phạm vi và giới hạn

- Chứng minh idempotency của nhánh đã publish khi chạy lại trong cùng run,
  với context, ETL và output vẫn nguyên vẹn. Không khẳng định toàn DAG idempotent.
- Với demo nhánh ở trên, chỉ Clear nhánh để quan sát riêng `BRANCH_REUSED`.
  Demo Clear ETL dùng quy trình bổ sung đầu tài liệu; nguồn thay đổi vẫn
  cần DagRun mới, không tự ghi đè generation đã được sử dụng.
- `BRANCH_PUBLISHED` ở attempt mới là kiểm tra/tái sử dụng snapshot, không có
  nghĩa một generation mới được tạo.
- Ưu tiên all-in-one vì không phát Asset. Với `ecommerce_etl_dag`, task thành
  công lại có thể phát thêm Asset event dù dữ liệu không đổi. Demo này không
  chứng minh event hay hành động downstream được thực hiện đúng một lần.
- Không cần Terminal hoặc sửa dữ liệu. Demo UI thực tế do người dùng thực hiện;
  kiểm thử fixture của publication sử dụng Spark mock.
