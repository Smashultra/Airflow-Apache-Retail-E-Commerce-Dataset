# Hướng dẫn đọc EDA RFM và so sánh k

Hai notebook trả lời hai câu hỏi: **khách hàng trong snapshot đang có hành vi R/F/M thế nào**, và **k=2,3,4 thay đổi cách phân khúc ra sao?**

Đọc [EDA_RFM_Parquet.ipynb](EDA_RFM_Parquet.ipynb) trước, rồi [comparison_revised.ipynb](comparison_revised.ipynb). Mỗi notebook có các mục A–G để đi từ nguồn dữ liệu đến diễn giải và kiểm chứng, tương tự guide anomalies.

## Chuẩn bị và chạy

1. Lưu các thay đổi trong editor trước khi mở bản notebook đã tổ chức lại.
2. Chọn kernel có `pandas`, `pyarrow`, `numpy`, `matplotlib`, `seaborn`. Nếu thiếu, chạy `%pip install pandas pyarrow numpy matplotlib seaborn`, rồi restart kernel.
3. Kiểm tra `PROJECT_ROOT_OVERRIDE`, `RUN_DATE` và các đường dẫn ở mục A. Mặc định ngày chốt là `2011-12-10`; tự dò repo từ thư mục hiện tại hoặc các thư mục cha.
4. Chạy **Restart Kernel → Run All** để tái tạo đúng thứ tự. Các notebook đọc pandas vào RAM; không cần chạy Spark để xem kết quả đã có.

| Nguồn mặc định | Đơn vị một dòng | Vai trò |
|---|---|---|
| `data/curated/RFM.parquet` | Dòng hóa đơn | Đầu vào giao dịch đã cleaning |
| `data/analytics/rfm_daily/run_date=2011-12-10` | Khách hàng | Snapshot RFM để EDA |
| `data/analytics/rfm_comparison/k={2,3,4}/run_date=2011-12-10` | Khách hàng | Assignment của các mô hình fit riêng |
| `data/analytics/k_selection/2011-12-10/k_scores.csv` | Phương án k | WSSSE và silhouette của lần k-selection |

Spark ghi Parquet dạng thư mục nhiều part file: truyền cả thư mục vào `read_parquet`. Đây là các đường dẫn legacy/local. DAG chính thức có đầu ra versioned và publication manifest; nếu dùng chúng, lấy đúng đường dẫn từ manifest rồi sửa cấu hình notebook. Không mặc định dữ liệu local là kết quả của DAG mới nhất, không trộn phiên bản nguồn.

Notebook không chạy cleaning, không fit lại K-Means và không ghi Parquet. Bản sao hai notebook trước chỉnh sửa được giữ tại `C:/Users/Lenovo/AppData/Local/Temp/rfm-notebooks-20261003-8bjem3i2` trên máy hiện tại.

## I. EDA_RFM_Parquet.ipynb

### A. Xác định đúng nguồn

`df` chứa dòng giao dịch; `rfm` chứa khách hàng. Tên `RFM.parquet` của curated không có nghĩa mỗi dòng đã là Recency/Frequency/Monetary. Xem đường dẫn in ra trước khi đọc bảng; đổi `RFM_OUTPUT_PATH` nếu snapshot nằm ở nơi khác.

### B. Chất lượng và bối cảnh giao dịch

Schema/null/trùng kiểm tra toàn bộ tập đang đọc. Bảng cancellation, Quantity/UnitPrice âm, bằng 0 hoặc không chuyển được thành số giúp phát hiện đầu ra cũ chưa đáp ứng cleaning hiện tại. Job cleaning hiện tại loại thiếu giá trị, hóa đơn C, số lượng/giá không dương và các dòng phí đã quy định; kết quả kiểm tra mới là bằng chứng cho file đang đọc.

Country/Description đếm **dòng giao dịch**. Một khách mua nhiều dòng có thể đóng góp nhiều lần; không đọc đó là thị phần khách hàng. `head` và top đơn giá là mẫu xem xét, không phải danh sách lỗi đã xác nhận.

Đổi `CUSTOMER_ID` ở B1 để đối soát lịch sử. Số hóa đơn trong toàn bộ curated có thể khác Frequency của snapshot vì RFM chỉ lấy giao dịch trước ngày chốt.

### C. Hiểu R/F/M và kiểm tra snapshot

| Chỉ số | Cách tính trong job local hiện tại | Cách đọc |
|---|---|---|
| Recency | Ngày chốt trừ ngày mua cuối | Thấp hơn nghĩa mua gần đây hơn |
| Frequency | Số InvoiceNo khác nhau trong lịch sử | Không phải số dòng, số sản phẩm hoặc số ngày mua |
| Monetary | Tổng Quantity × UnitPrice, với phép tính decimal và làm tròn tổng đến 2 chữ số | Giá trị mua trong cohort đã lọc, không phải lợi nhuận |

Giao dịch tại đúng ngày chốt bị loại. Với ngày chốt `2011-12-10`, lịch sử chỉ chứa giao dịch trước ngày đó. Notebook kiểm tra ID khách duy nhất, metric hữu hạn, Recency/Frequency ít nhất 1, Monetary không âm, score 1–5 và mã cụm nguyên hợp lệ. Nếu có `run_date`/`LastPurchaseDate`, kiểm tra ngày và đối chiếu Recency.

Monetary chuyển sang số thực trong pandas phục vụ thống kê và biểu đồ. Không dùng số thực này thay phép kiểm chứng tiền tệ decimal của Spark. Mẫu ngẫu nhiên dùng seed 42 và tự giảm kích thước khi tập nhỏ.

### D. Phân phối và điểm tương đối

Histogram thang gốc cho thấy khách có Frequency/Monetary lớn có thể kéo đuôi phân phối. `log1p(x) = ln(1+x)` giúp nhìn phần đông khách rõ hơn. Histogram/KDE phụ thuộc bins và cách làm trơn; hình dạng biểu đồ chưa chứng minh số cụm tối ưu.

Job K-Means local dùng log1p của R/F/M rồi StandardScaler trước khi fit. **R_score/F_score/M_score không phải feature đầu vào K-Means.** Các score dùng percent_rank trong cohort: Recency thấp tốt hơn, Frequency/Monetary cao tốt hơn. Ties được giữ; mức score không nhất thiết chia khách thành năm phần bằng nhau. Metric hằng nhận score 1. RFM_score là mã ghép ba score, không phải đại lượng để cộng/trung bình hay khoảng cách phân cụm.

### E. Quy mô và profile cụm

Đọc mean, median, Q1/Q3 cùng nhau. Mean Monetary cao có thể do một số khách chi rất lớn; median mô tả khách ở giữa. `one_purchase_pct` là tỷ lệ khách có Frequency=1 trong chính cụm. `customer_pct` dùng tổng khách của snapshot làm mẫu số; `monetary_pct` dùng tổng Monetary của snapshot.

Heatmap chuẩn hóa trung bình giữa các cụm cho mỗi metric, đảo chiều Recency để **xanh luôn là phía thuận lợi hơn**. Số trên ô vẫn là trung bình gốc. Màu là so sánh tương đối, không phải ngưỡng nghiệp vụ hay tâm cụm trong không gian feature của mô hình. Cột không phân tán được tô trung tính.

Boxplot ẩn điểm ngoại lệ để nhìn hộp rõ hơn, nhưng thống kê vẫn giữ mọi khách. Scatter giữ toàn bộ khách; điểm chồng lên nhau hoặc trục Monetary dài có thể che cấu trúc phần đông.

### F. Đặt nhãn theo profile

Cluster ID không có thứ tự giá trị và có thể đổi giữa các lần fit. Notebook xếp Monetary median tăng dần, phá hòa bằng Frequency median tăng dần rồi Recency median giảm dần. Hai đầu gọi Lower-value/Higher-value; k=3 thêm Intermediate. Với k=4, hai cụm giữa được gọi Recent Moderate/Less-recent Moderate theo Recency median.

Nhãn lưu trong `EDA_Segment`; giữ nguyên `Segment` của pipeline nếu có. Quy tắc hỗ trợ k=2,3,4, dừng khi profile hòa không thể đặt tên rõ. Nhãn mang nghĩa tương đối trong mỗi phương án, không khẳng định khách mới, khách VIP hay khách đã rời bỏ. “Mua gần đây” không chứng minh “mới gia nhập” vì chưa xét ngày mua đầu; Recency cao chỉ cho thấy chưa mua lại trong khoảng quan sát.

### G. Kiểm chứng và sử dụng kết quả

Assertion kiểm tra hợp đồng đầu vào và tổng profile. Khi một assertion lỗi, xem nguồn/ngày/schema trước khi bỏ kiểm tra. Tổng khách đúng chưa chứng minh RFM đúng so với raw; notebook này không tái tính độc lập toàn bộ RFM hay đánh giá hiệu quả chiến dịch.

## II. comparison_revised.ipynb

### A–B. Bảo đảm so sánh công bằng

Ba phương án phải có cùng CustomerID và từng giá trị Recency/Frequency/Monetary. Notebook sắp theo CustomerID rồi dùng `assert_frame_equal`, thay vì chỉ so số dòng. Mã Cluster phải nguyên, đủ k cụm và trong khoảng 0 đến k−1; ngày chốt nếu có phải khớp cấu hình.

Đổi membership giữa các k chỉ có nghĩa khi cohort/RFM khớp. Các mô hình fit riêng; k=4 không bắt buộc là k=3 được tách một cụm.

### C. Đọc profile và nhãn

Quy tắc nhãn giống EDA, cùng dùng median để tránh phụ thuộc ID. Tên Higher-value ở k=2 và k=4 không có nghĩa hai tập khách hoàn toàn giống nhau. Xem quy mô, tỷ trọng Monetary và R/F/M trước khi đề xuất hành động.

### D. Transition hai chiều

| Bảng | Mẫu số | Câu hỏi |
|---|---|---|
| Số lượng | Không chuẩn hóa | Bao nhiêu khách có cặp nhãn đó? |
| % theo hàng | Tổng khách của nhóm tại k cũ | Khách của nhóm cũ phân bổ sang đâu? |
| % theo cột | Tổng khách của nhóm tại k mới | Nhóm mới nhận khách từ nhóm nào? |

Một ô có tỷ lệ hàng lớn chưa chắc đóng góp phần lớn nhóm mới. Đây là đối chiếu các mô hình trên **cùng snapshot**, không phải khách thay đổi hành vi theo thời gian. Không diễn giải transition thành churn, nâng hạng hoặc khách được can thiệp thành công.

### E. Tập con Intermediate k=3

Notebook chọn bằng nhãn Intermediate, rồi đối chiếu cùng khách ở k=4. Bảng giữ tất cả khách; boxplot chi tiết chỉ giữ nhóm chiếm ít nhất `MIN_DETAIL_PCT`, mặc định 1% số khách Intermediate. Nhóm nhỏ vẫn nằm trong bảng và tổng, không bị loại khỏi cohort.

Boxplot Monetary dùng log1p và giữ điểm ngoại lệ. Histogram/KDE Recency dùng toàn bộ Intermediate. Một lát cắt Recency không phản ánh đầy đủ không gian RFM ba chiều; không có hai đỉnh rõ không bác bỏ k=4, có hai đỉnh cũng không chứng minh k=4 tối ưu. Không dùng kiểm định trên chính feature phân cụm để tuyên bố khác biệt đã được xác nhận độc lập.

### F. WSSSE, silhouette và mục tiêu chọn k

WSSSE là tổng khoảng cách bình phương trong cụm: thường giảm khi k tăng, nên k có WSSSE nhỏ nhất chưa chắc hữu ích. Silhouette cao hơn cho thấy tách/khít hơn theo khoảng cách đang sử dụng. Hai chỉ số phải được cân nhắc cùng quy mô cụm, profile dễ giải thích và mục tiêu hành động.

CSV hiện tại không mang đủ provenance để notebook chứng minh cùng seed/fingerprint với ba assignment. Đọc nó như bằng chứng của lần k-selection đã lưu, đối chiếu cấu hình trước khi kết hợp. Notebook không fit lại để tính score và không đo ổn định nhiều seed.

### G. Snapshot được kiểm tra ngày 2026-10-03

Cả ba phương án local cho ngày chốt `2011-12-10` có **4.334 khách**, cùng cohort và cùng RFM. Các số dưới đây là kết quả file hiện có; nếu đổi dữ liệu phải chạy lại và cập nhật nhận định.

| k | Nhãn | Khách | Median R | Median F | Median M |
|---:|---|---:|---:|---:|---:|
| 2 | Lower-value | 2.671 | 97 | 1 | 356,85 |
| 2 | Higher-value | 1.663 | 17 | 6 | 2.035,08 |
| 3 | Lower-value | 1.872 | 160 | 1 | 292,735 |
| 3 | Intermediate | 1.709 | 30 | 3 | 958,71 |
| 3 | Higher-value | 753 | 10 | 10 | 3.640,67 |
| 4 | Lower-value | 1.528 | 159,5 | 1 | 236,03 |
| 4 | Less-recent Moderate | 1.050 | 82,5 | 3 | 1.018,60 |
| 4 | Recent Moderate | 1.065 | 17 | 3 | 812,68 |
| 4 | Higher-value | 691 | 10 | 10 | 3.901,81 |

Median tiền có thể có 3 chữ số thập phân khi lấy trung bình hai quan sát giữa, dù mỗi khách được làm tròn đến 2 chữ số.

Trong Intermediate k=3, k=4 phân bổ 701 khách vào Less-recent Moderate và 991 vào Recent Moderate; 12 và 5 khách vào hai nhóm còn lại. Hai nhóm nhỏ dưới 1% không vào boxplot chi tiết. Median Recency của hai **tập con Intermediate** là 68 và 18 ngày, khác median 82,5 và 17 của **toàn cụm k=4**. Luôn xác định đúng tập khách trước khi trích số.

CSV đã lưu có silhouette k=2 khoảng **0,6238**, k=3 **0,5005**, k=4 **0,4918**. k=2 cao hơn theo chỉ số này; k=4 vẫn có thể được cân nhắc vì tách hai nhóm moderate theo mức độ gần đây. Kết luận phù hợp là “k=4 hữu ích cho mục tiêu diễn giải đang xét”, chưa phải “k=4 tối ưu đã được chứng minh”.

## Kiểm chứng của bản tổ chức lại

- Hai notebook đã chạy bằng `nbclient.NotebookClient(...).execute()` từ thư mục `notebooks`: EDA 17 cell code, comparison 9 cell code, không có output lỗi.
- `nbformat.validate` và compile từng cell code thành công.
- Kiểm tra tập trung cho `semantic_mapping`: đổi ID cụm vẫn giữ ý nghĩa nhãn, hai nhóm moderate hòa Recency bị từ chối, áp dụng cho cả hai notebook.
- SHA256 trước/sau của 17 file đầu vào (Parquet và CSV score) không đổi. Không sửa dữ liệu, chạy lại Spark, commit hay push.

## Các nhầm lẫn thường gặp

| Cách hiểu dễ nhầm | Cách đọc đúng |
|---|---|
| RFM.parquet đã là bảng khách | Curated RFM.parquet vẫn là dòng giao dịch |
| Frequency là số dòng mua | Frequency đếm hóa đơn duy nhất |
| Cluster 1 luôn là nhóm tốt nhất | Đọc profile, ID chỉ dùng truy vết |
| Các score chia đều 20% khách | Ties và percent_rank có thể tạo nhóm lệch |
| Recent Moderate là khách mới | Chỉ biết Recency thấp hơn nhóm moderate còn lại |
| Transition là hành vi thay đổi theo ngày | Đang đối chiếu mô hình khác k trên cùng snapshot |
| Boxplot không vẽ ngoại lệ là đã bỏ khách | Khách vẫn có trong thống kê và profile |
| k lớn hơn/WSSSE thấp hơn luôn tốt hơn | Cần cân nhắc score, ổn định và mục tiêu sử dụng |
| RFM cao chứng minh hiệu quả marketing | Chưa có đo lường can thiệp hoặc nhãn kết quả |

Trước khi đưa vào báo cáo, ghi ngày chốt, nguồn/phiên bản, k, đơn vị đếm và mẫu số; diễn giải profile rồi nêu giới hạn của kết luận.
