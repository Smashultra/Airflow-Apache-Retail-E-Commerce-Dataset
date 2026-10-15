# Demo sensor deadlock trên UI

Một file DAG riêng, không sửa pipeline nghiệp vụ, tạo ba DAG mỗi DAG một task:
`retail_sensor_demo_poke`, `retail_sensor_demo_reschedule`,
`retail_sensor_demo_producer`. Hai DAG sensor chỉ khác mode.

## Chuẩn bị một lần trên UI

1. Đợi DAG processor nạp file mới. Vào trang quản lý **Pools**, tạo pool
   `retail_sensor_demo` với **1 slot**. Không đổi default_pool hoặc retail_spark.
2. Unpause ba DAG demo. Chúng có schedule=None, chỉ chạy khi trigger thủ công.
3. Dùng logical date hiện tại hoặc mặc định UI; các DAG demo không có end_date
   lịch sử và không dùng business_date. Không chạy hai lượt demo đồng thời.

## Lượt 1: quan sát kẹt tài nguyên với poke

1. Trigger `retail_sensor_demo_poke`, Param `demo_key=blocked_01`.
   Mỗi lần demo phải dùng key mới, chưa từng được producer tạo.
2. Đợi `wait_for_signal` **Running**, mở log thấy
   `SENSOR_DEMO key=blocked_01 ready=False`.
3. Ngay trong vòng 3 phút, trigger `retail_sensor_demo_producer` với cùng
   `demo_key=blocked_01`.
4. Quan sát producer chưa chạy (thường Scheduled, có thể hiển thị Queued tùy
   thời điểm); trang Pools cho thấy slot duy nhất bị sensor chiếm.

```text
Sensor giữ slot → chờ file do producer tạo
Producer chờ slot → không thể tạo file
```

Đây là vòng chờ tài nguyên giữa hai DAG, không phải chu trình dependency trong
một DAG. Sensor đang Running nhưng không thể đạt điều kiện khi producer bị chặn.

5. Sau khoảng 180 giây chờ của sensor (có thể lâu hơn một nhịp poke/scheduler),
   sensor timeout và Failed, giải phóng slot. Producer sau đó chạy Success.
   Đợi cả hai run kết thúc trước lượt tiếp theo. Không cần kill container hay
   Mark success. Timeout là giới hạn bảo vệ demo, không phải giải pháp gốc.

## Lượt 2: reschedule phá vòng chờ

1. Trigger `retail_sensor_demo_reschedule` với key mới `released_01`.
2. Đợi log `ready=False` và trạng thái **Up for reschedule**.
3. Trigger `retail_sensor_demo_producer` với cùng `demo_key=released_01`.
4. Producer lấy slot giữa các lần sensor kiểm tra và Success. Sensor kiểm tra
   lại sau khoảng 15 giây cộng độ trễ scheduler, thấy `ready=True` và Success.

| Quan sát | poke | reschedule |
|---|---|---|
| Khi chưa có file | Running, giữ slot | Up for reschedule, nhả slot |
| Producer trong pool 1 slot | Bị chặn đến khi sensor timeout | Có thể chạy giữa các lần kiểm tra |
| Kết quả sensor trong demo | Failed vì timeout | Success khi file xuất hiện |

Câu trình bày: “Sensor poke giữ slot trong lúc chờ; nếu producer cũng cần slot
đó thì không bên nào tiến triển. Reschedule trả slot trong thời gian chờ,
cho producer chạy để đáp ứng điều kiện của sensor.”

## Lưu ý và liên hệ project

- LandingValidationSensor ở hai DAG daily đã cấu hình mode=reschedule.
  Demo tạo tình huống xấu riêng để giải thích lựa chọn này; không kết luận
  pipeline hiện tại đang deadlock.
- Producer ghi JSON nhỏ dưới data/demos/sensor_deadlock/, tên file là hash
  của demo_key. Không cần xóa file: đổi key cho mỗi lượt. Cùng key cũ có thể
  khiến sensor thành công ngay và mất tình huống demo.
- Nếu pool chưa được tạo, cả hai task có thể không chạy: đó là thiếu pool,
  chưa phải tình huống sensor giữ slot. Nếu pool có hơn 1 slot, demo poke
  không còn tạo vòng chờ như thiết kế.
- Với reschedule, cần scheduler/executor còn hoạt động và không bị tải khác
  chiếm hết khả năng thực thi. Đây không phải demo deferrable/triggerer.
- Pause ba DAG sau demo. Không cần thay cấu hình Docker hay sửa code giữa hai lượt.
- Kiểm thử fixture kiểm tra key/path, mode, timeout, pool và DAG serialization;
  chưa thay thế quan sát thực tế scheduler/pool trên UI.
