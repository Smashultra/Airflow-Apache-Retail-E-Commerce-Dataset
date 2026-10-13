# DAG ecommerce_etl_dag_allinone

Phiên bản gộp pipeline daily và hai consumer vào **một DAG gồm 8 task**.
Lịch chạy là cron ngày, không sử dụng Asset-aware scheduling, không khai báo
inlets/outlets và không phát Asset event. Các DAG cũ được giữ để đối chiếu.

```mermaid
flowchart LR
    S[start_pipeline] --> V[validate_raw_data]
    V --> E[submit_pyspark_etl]
    E --> R[compute_rfm_metrics]
    E --> A[detect_anomalies]
    R --> M[execute_voucher_campaign]
    R --> N[notify_completion]
    A --> N
    N --> D[generate_executive_summary]
```

- RFM và Anomaly giữ nguyên Spark job, kiểm tra chất lượng, publication marker
  và cơ chế tái sử dụng snapshot khi retry. Chỉ bỏ việc phát Asset.
- Marketing dùng manifest RFM của đúng DagRun, chạy sau RFM mà không chờ Audit.
- Notify chờ cả hai nhánh, đối soát snapshot và ghi completion. Metadata nhỏ
  được chuyển bằng XCom; Parquet vẫn nằm trên filesystem, không đưa vào XCom.
- Dashboard đọc XCom của notify trong cùng DagRun, kiểm tra hash, marker,
  model và các output `_SUCCESS`. Không đọc con trỏ ngày mới nhất.
- Tất cả task dùng `all_success`. Hai leaf là Marketing và Dashboard: lỗi
  consumer vẫn làm DAG thất bại; Audit lỗi chặn notify/Dashboard nhưng không
  chặn Marketing nếu RFM đã thành công. Có thể retry riêng task thất bại.
- Marketing/Dashboard vẫn là demo ghi log, chưa gửi voucher/email hoặc cập
  nhật dashboard bên ngoài.

Giữ lịch `0 0 * * *` UTC với khoảng lịch sử 2010–2011, `catchup=False`, mặc
định paused, một active run, cùng pool `retail_spark` một slot và driver 3g.
Nhánh độc lập trong graph không có nghĩa hai Spark JVM chạy song song.
OpenLineage và Spark listener tùy chọn tiếp tục hoạt động; lineage metadata
không phải Asset scheduling.

Chuẩn bị landing, pool và model như [SETUP.md](SETUP.md). Chạy thủ công DAG
`ecommerce_etl_dag_allinone` với `{"business_date":"2011-12-09"}`; có thể ghim
`model_version`. Training vẫn độc lập theo tháng vì không dùng Asset schedule
và daily cần model đã huấn luyện với cutoff hợp lệ trước ngày scoring.

Khi demo, chọn một trong daily cũ và all-in-one để tránh xử lý trùng cùng
ngày. Output theo run được tách bằng DAG ID/run ID; con trỏ published theo
ngày vẫn dùng chung. All-in-one không kích hoạt hai consumer DAG cũ.

Kiểm chứng: `tests/test_allinone_dag.py` kiểm tra graph, không có outlets,
serialization, Marketing chạy khi thiếu Audit, Dashboard chờ completion,
retry, lỗi Spark, loại snapshot khác run và lineage. Chưa chạy toàn dataset
hoặc gửi bất kỳ chiến dịch Marketing thực tế nào bằng DAG mới.
