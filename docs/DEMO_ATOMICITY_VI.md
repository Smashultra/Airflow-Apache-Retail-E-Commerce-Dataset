# Demo atomicity trên Airflow UI

## Phạm vi chứng minh

Demo kiểm chứng ranh giới công bố của nhánh audit và snapshot tổng hợp:
Spark có thể đã ghi Parquet và audit.json thành công, nhưng task lỗi trước
publication thì chưa có audit_publication.json, chưa có completion.json của
run và chưa có Asset snapshot cho Dashboard. Đây không phải rollback toàn bộ
DAG hoặc mô phỏng lỗi giữa lúc ghi từng file Parquet.

Thay đổi nhỏ: thêm Param `demo_fail_audit_once=false` vào hai DAG daily và một
điểm gây lỗi trong BranchSparkSubmitOperator sau Spark, trước publication.
Chỉ tác động nhánh audit của manual run khi Param bật. AirflowFailException
dừng task không auto-retry. Marker riêng theo run `demo_audit_failure.json`
ghi nhận lỗi demo đã được kích hoạt; đây KHÔNG phải marker dữ liệu sẵn sàng.

## Demo gọn nhất: all-in-one

1. Chuẩn bị landing, model hợp lệ và pool như demo bình thường. Đợi DAG parse
   cập nhật, mở ecommerce_etl_dag_allinone, Unpause, Trigger một run mới.
2. Chọn logical date lịch sử chưa dùng, ví dụ 2011-12-09T12:00:00Z
   (19:00 cùng ngày nếu UI ở UTC+07). End date daily bao gồm hết 09/12 UTC.
   Run ID gợi ý `atomicity_demo_01`. Nhập Params:

```json
{
  "business_date": "2011-12-09",
  "source_version": "online-retail-v1",
  "rfm_k": null,
  "model_version": null,
  "churn_days": 90,
  "enable_marketing": true,
  "enable_dashboard": true,
  "demo_fail_audit_once": true
}
```

3. Chờ detect_anomalies Failed và các task còn chạy kết thúc. Mở log audit,
   tìm `ATOMICITY_DEMO`. Spark đã hoàn tất; lỗi có chủ đích xảy ra trước khi
   operator công bố nhánh. Log nhánh audit chưa có `BRANCH_PUBLISHED audit`
   trong attempt lỗi này.
4. Graph: ETL thành công; RFM/Marketing có thể hoàn tất độc lập; audit failed;
   notify_completion, check_dashboard_enabled, Dashboard và finish_pipeline
   bị chặn bởi lỗi upstream. Không gọi Mark success để vượt qua lỗi.
5. Muốn bổ sung bằng chứng filesystem: lấy context_path ở XCom validation,
   mở thư mục tương ứng trong VS Code. Có audit.json và output Parquet,
   có demo_audit_failure.json; chưa có audit_publication.json/completion.json.
   Nếu đã có snapshot được công bố trước demo, pointer theo ngày vẫn giữ
   snapshot đó, với điều kiện không có run khác đang công bố cùng cutoff.
6. Trên UI chọn detect_anomalies → Clear; chọn cả Downstream của run hiện tại.
   Không chọn Upstream, Past/Future hoặc toàn DAG. Kiểm tra danh sách preview:
   audit, completion, check_dashboard_enabled, Dashboard, finish_pipeline.
   Để DAG unpaused và cho phép run trở lại running nếu UI hỏi.
7. Giữ nguyên Params, không tắt cờ demo và không sửa code giữa hai attempt.
   Marker đã tồn tại nên lần này audit chạy Spark lại rồi công bố thành công.
   Completion và Dashboard chạy, finish_pipeline thành công.
8. Kiểm tra attempt mới có `BRANCH_PUBLISHED audit`; completion có
   `PIPELINE_COMPLETE`; Dashboard có `EXECUTIVE_DEMO`. Lúc này mới có
   audit_publication.json và completion.json của run.

Không cần clear ETL/RFM trong demo phục hồi này. ETL đã hỗ trợ reuse nếu
đầu vào/output hợp lệ; run đã lệch generation từ trước cần tạo run mới.
Output audit chưa publish ở attempt đầu có thể còn trên đĩa; kết quả
được công bố sau phục hồi phải được đọc qua manifest. Không xóa marker để lặp
demo; tạo DagRun mới với run ID/logical date chưa dùng.

## Biến thể chứng minh Asset-aware scheduling

Dùng ecommerce_etl_dag với cùng Params nguồn/ngày/K/cờ demo, bỏ hai Params
enable_marketing/enable_dashboard. Unpause hai consumer trước khi chạy.
Lần lỗi chưa có flagged_anomalies event của nhánh audit và retail_snapshot
event của run đó; Marketing vẫn có thể được RFM kích hoạt. Sau khi Clear
detect_anomalies + notify_completion và chạy thành công, Asset snapshot mới
kích hoạt Dashboard. So sánh source_run_id để tránh nhầm với event/run cũ.

## Giới hạn và verification

Mặc định false; scheduled/backfill không kích hoạt lỗi này. Lần Clear vẫn chạy
lại Spark audit (không tối ưu tái sử dụng stage chưa publish). Marker chỉ phục
vụ demo một lần và không ảnh hưởng nội dung dữ liệu. Nếu tiến trình bị dừng
ngay sau khi ghi marker nhưng trước khi raise, không bảo đảm UI hiển thị đúng
lỗi chủ động; đây là công cụ demo có kiểm soát, không phải fault-injection framework.

Kiểm thử fixture: tests/test_branch_publication.py kiểm tra Spark được gọi
trước lỗi, không audit publication/Asset/completion, pointer cũ giữ nguyên,
Clear mô phỏng phục hồi, retry sau publish tái sử dụng marker và chế độ mặc
định/scheduled không bị ảnh hưởng. Chưa chạy demo lỗi trên dữ liệu thật.
