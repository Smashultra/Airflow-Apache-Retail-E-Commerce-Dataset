# Hướng dẫn đọc EDA Online Retail

Tài liệu đi kèm [EDA_Online_Retail.ipynb](EDA_Online_Retail.ipynb), chuyển từ bản tổng hợp Word và đối chiếu với code, output đã lưu cùng CSV của dự án ngày **03/10/2026**. Giữ đủ 12 mục, bổ sung cách đọc histogram còn thiếu cùng phần giải thích chi tiết về làm sạch. Guide trình bày bảng số liệu và diễn giải; xem các biểu đồ trực tiếp trong notebook ở mục tương ứng.

### Đối chiếu mục trong guide với notebook

| Guide này | Mục tương ứng trong notebook |
| --- | --- |
| 1. Giới thiệu | Phần mở đầu |
| 2. Tổng quan cấu trúc | 1. Tải dữ liệu và tổng quan cấu trúc |
| 3. Giá trị thiếu | 2. Giá trị thiếu |
| 4. Làm sạch và phát hiện bất thường | 3. Làm sạch dữ liệu & phát hiện bất thường |
| 5. Giá trị đặc biệt | Phần giải thích bổ sung từ các output và CSV |
| 6. Phân phối các biến số | **4. Phân phối Quantity, UnitPrice, Revenue** |
| 7–10. Thời gian, quốc gia, sản phẩm, khách hàng | 5–8 của notebook |
| 11. Tổng kết | 9. Tổng kết insight chính |
| 12. Lưu ý | Giới hạn khi đọc và sử dụng kết quả |

**Phần thiếu được bổ sung:** mục 4 của notebook có ba phần: histogram, boxplot và bảng phân vị. Word đã có hai phần sau ở mục 6, nhưng thiếu phần giải thích histogram. Guide bổ sung cách đọc biểu đồ này. Mục 4 của guide cũng được mở rộng để giải thích đầy đủ 2,515 dòng giá bằng 0, hai dòng giá âm và phạm vi bộ lọc.

### Chuẩn bị và đọc notebook

Notebook dùng `pandas`, `numpy`, `matplotlib`, `seaborn`. Code hiện đọc `pd.read_csv("data.csv", encoding="ISO-8859-1")`, nghĩa là tìm CSV trong thư mục làm việc của kernel. Trong repository, nguồn nằm tại `data/raw/data.csv`: nếu kernel chạy từ `notebooks/`, dùng `../data/raw/data.csv`; nếu chạy từ gốc repository, dùng `data/raw/data.csv`.

Sau khi chọn đúng nguồn, chạy **Restart Kernel → Run All** từ trên xuống. Mục thời gian tạm đặt `InvoiceDate` làm index rồi reset lại; chạy riêng các cell sai thứ tự có thể gây lỗi. Bản guide này đối chiếu output đã lưu và tính lại các thống kê từ CSV, không chạy lại hay sửa notebook.

## 1. Giới thiệu

Bộ dữ liệu ghi lại các giao dịch bán lẻ trực tuyến của một công ty tại Anh trong giai đoạn 01/12/2010 – 09/12/2011. Mỗi dòng là một dòng hóa đơn (invoice line), gồm mã hóa đơn, mã sản phẩm, mô tả, số lượng, ngày giao dịch, đơn giá, mã khách hàng và quốc gia.

Notebook lần lượt xem cấu trúc dữ liệu và giá trị thiếu, làm sạch và phát hiện bất thường, phân tích phân phối các biến số, rồi phân tích theo thời gian, quốc gia, sản phẩm và khách hàng. Tài liệu này tổng hợp lại kết quả theo đúng trình tự đó, và bổ sung mục 5 giải thích ý nghĩa các giá trị đặc biệt trong Description, StockCode và InvoiceNo. Số liệu giữ định dạng của notebook (dấu phẩy phân cách hàng nghìn, dấu chấm thập phân); đơn vị tiền tệ là GBP.

Các con số chính

| 541,909<br>dòng hóa đơn, 8 cột | 25,900<br>hóa đơn duy nhất | 4,070<br>mã sản phẩm | 38<br>quốc gia |
| --- | --- | --- | --- |
| 530,104<br>dòng sau làm sạch (97.8%) | ≈ 10.67 triệu<br>GBP doanh thu sau làm sạch | 84.6%<br>doanh thu đến từ United Kingdom | 74.6%<br>doanh thu từ top 20% khách hàng |

Tính trực tiếp `sum(Quantity * UnitPrice)` trên đúng bộ lọc của notebook cho **10,666,684.544 GBP**, xấp xỉ **10.67 triệu GBP**. Đây là tổng giá trị dòng dương được giữ lại, còn chứa phí và chưa đối trừ hoàn/hủy; không đồng nghĩa doanh thu thuần. Tỷ lệ 74.6% dùng riêng tổng giá trị của nhóm **có CustomerID** làm mẫu số.

## 2. Tổng quan cấu trúc dữ liệu

Dữ liệu được đọc từ data.csv với encoding ISO-8859-1, gồm 541,909 dòng và 8 cột, chiếm khoảng 33.1 MB bộ nhớ.

Bảng 1. Các cột trong dữ liệu gốc

| Cột | Kiểu | Non-null | Giá trị duy nhất | Ghi chú |
| --- | --- | --- | --- | --- |
| InvoiceNo | object | 541,909 | 25,900 | Mã hóa đơn; bắt đầu bằng "C" là đơn hủy |
| StockCode | object | 541,909 | 4,070 | Mã sản phẩm; lẫn mã phi sản phẩm (POST, DOT, M) |
| Description | object | 540,455 | 4,223 | Phổ biến nhất: WHITE HANGING HEART T-LIGHT HOLDER (2,369 dòng) |
| Quantity | int64 | 541,909 | – | Từ −80,995 đến 80,995; trung vị 3 |
| InvoiceDate | object | 541,909 | 23,260 | Dạng chuỗi, được chuyển sang datetime |
| UnitPrice | float64 | 541,909 | – | Từ −11,062.06 đến 38,970; trung vị 2.08 |
| CustomerID | float64 | 406,829 | – | Có NaN; mã chạy từ 12346 đến 18287 |
| Country | object | 541,909 | 38 | United Kingdom chiếm 495,478 dòng (91.4%) |

Một vài điểm đáng chú ý ở dữ liệu thô:

- Quantity có trung bình 9.55 nhưng độ lệch chuẩn 218.08; UnitPrice có trung bình 4.61 và độ lệch chuẩn 96.76. Cả hai đều có giá trị âm và giá trị cực lớn.

- Hóa đơn nhiều dòng nhất là 573585 với 1,114 dòng, lập lúc 14:41 ngày 31/10/2011.

- Mã sản phẩm xuất hiện nhiều nhất là 85123A (2,313 dòng).

## 3. Giá trị thiếu

Chỉ có hai cột chứa giá trị thiếu.

Bảng 2. Giá trị thiếu theo cột

| Cột | Số dòng thiếu | Tỷ lệ | Hướng xử lý notebook đề xuất |
| --- | --- | --- | --- |
| CustomerID | 135,080 | 24.93% | Loại bỏ khi phân tích theo khách hàng (RFM, phân khúc) |
| Description | 1,454 | 0.27% | Loại bỏ hoặc điền "Unknown" |

Biểu đồ thanh ngang ở mục 2 của notebook thể hiện tỷ lệ thiếu trên tổng số dòng; cột CustomerID nổi bật hơn nhiều so với Description.

Các dòng thiếu CustomerID là giao dịch không xác định được khách hàng. Bán tại quầy/khách vãng lai chỉ là giả thuyết; dữ liệu không xác nhận nguyên nhân thiếu ID. Notebook chỉ bỏ thiếu ID ở phần phân tích khách hàng; đề xuất điền Description bằng "Unknown" chưa được thực hiện trong code.

## 4. Làm sạch dữ liệu và phát hiện bất thường

InvoiceDate được chuyển sang kiểu datetime. Notebook thêm cột cờ is_cancelled (hóa đơn có mã bắt đầu bằng "C") và cột Revenue = Quantity × UnitPrice.

### 4.1. Phân biệt dòng hủy, số lượng âm và giá không dương

Bảng 3. Các bất thường trong dữ liệu gốc

| Kiểm tra | Số dòng | Tỷ lệ trên dữ liệu gốc |
| --- | --- | --- |
| Giao dịch hủy (InvoiceNo bắt đầu bằng "C") | 9,288 | 1.71% |
| Quantity < 0 | 10,624 | 1.96% |
| UnitPrice = 0 | 2,515 | 0.46% |
| UnitPrice < 0 | 2 | < 0.01% |

Bảng chéo giữa is_cancelled và Quantity < 0 cho thấy:

- Toàn bộ 9,288 dòng hủy đều có số lượng âm. Đây là dấu hiệu phù hợp với hoàn/hủy đơn, không phải căn cứ tự động kết luận lỗi nhập liệu.

- Còn 1,336 dòng có số lượng âm nhưng không phải đơn hủy. Đối chiếu CSV xác nhận tất cả có UnitPrice = 0 và không có CustomerID. Các ví dụ mô tả trống, "check", "lost", "missing", "smashed" gợi ý điều chỉnh tồn kho; không có trường lý do để xác nhận từng dòng.

- Các nhóm kiểm tra có giao nhau: 9,288 dòng hủy nằm trong nhóm Quantity âm; 1,336 dòng âm còn lại nằm trong nhóm giá bằng 0. Không cộng các số trong Bảng 3 để tính tổng số dòng bị loại.

| is_cancelled | Quantity ≥ 0 | Quantity < 0 | Tổng số dòng |
| --- | ---: | ---: | ---: |
| False | 531,285 | 1,336 | 532,621 |
| True | 0 | 9,288 | 9,288 |

Tỷ lệ dòng hủy trong nhóm số lượng âm là `9,288 / 10,624 = 87.42%`. Cần phân biệt **dòng hóa đơn** với **hóa đơn**: 9,288 không phải số đơn hủy duy nhất.

### 4.2. Phần bổ sung: giải thích đủ 2,515 dòng UnitPrice = 0

Sau 1,336 dòng số lượng âm không hủy, còn **1,179 dòng** giá bằng 0. Đối chiếu CSV xác nhận cả 1,179 dòng đều có số lượng dương và không có tiền tố C. Chia nhóm theo CustomerID và việc **cùng InvoiceNo có ít nhất một dòng UnitPrice > 0 trong dữ liệu gốc**:

| Có CustomerID? | Cùng hóa đơn có dòng giá dương? | Số dòng giá 0 | Cách đọc |
| --- | --- | ---: | --- |
| Không | Không | 772 | Chưa có khách định danh hoặc dòng tính tiền cùng hóa đơn để đối chiếu |
| Không | Có | 367 | Có dòng giá 0 nằm cạnh dòng tính tiền, nhưng thiếu danh tính khách |
| Có | Không | 7 | Có khách, toàn bộ hóa đơn không có dòng giá dương; cần xem từng trường hợp |
| Có | Có | 33 | Dòng giá 0 đi cùng giao dịch tính tiền của khách đã định danh |

Kiểm tra tổng: **2,515 = 1,336 + 772 + 367 + 7 + 33**. Các số này tính trên CSV gốc, chưa loại trùng; không trộn với số liệu Parquet của guide anomalies.

Bảy dòng có khách nhưng không có dòng giá dương cùng hóa đơn thuộc bốn hóa đơn:

| InvoiceNo | CustomerID | Số dòng | Bằng chứng trong CSV |
| --- | --- | ---: | --- |
| 543599 | 17560 | 1 | 16 sản phẩm `84535B`, FAIRY CAKES NOTEBOOK A6 SIZE |
| 564651 | 14646 | 4 | Các mã `23270`, `23268`, `22955`, `21786`, số lượng lần lượt 96, 192, 144, 144 |
| 568384 | 12748 | 1 | Mã `M`, mô tả Manual, số lượng 1 |
| 578841 | 13256 | 1 | Mã `84826`, ASSTD DESIGN 3D PAPER STICKERS, số lượng 12,540 |

Đây là **phân tích bổ sung của guide**, chưa có bảng phân nhóm này trong notebook đang lưu. Quà tặng, khuyến mãi, điều chỉnh hoặc thiếu giá đều chỉ là khả năng; không thể chốt lý do từ giá 0. Những dòng này đóng góp 0 vào Revenue nhưng vẫn có thể ảnh hưởng số lượng và số đơn nếu giữ lại.

### 4.3. Hai dòng UnitPrice < 0

CSV có `A563186` và `A563187`, cùng ngày 12/08/2011, StockCode `B`, Description `Adjust bad debt`, Quantity = 1, UnitPrice = −11,062.06, thiếu CustomerID. Đây là ngữ cảnh điều chỉnh nợ xấu theo mô tả, không phải hàng bán giá âm thông thường. Dòng `A563185` cùng mã/mô tả nhưng giá **dương** 11,062.06 vẫn qua bộ lọc notebook.

### 4.4. Bộ lọc thực sự làm gì?

```python
df["Revenue"] = df["Quantity"] * df["UnitPrice"]
df_clean = df[
    (~df["is_cancelled"])
    & (df["UnitPrice"] > 0)
    & (df["Quantity"] > 0)
].copy()
```

Tập dữ liệu sạch df_clean giữ lại các dòng không hủy, có UnitPrice > 0 và Quantity > 0. Kết quả còn 530,104 dòng (97.8% dữ liệu gốc), tức loại 11,805 dòng. Mọi phân tích từ mục 6 trở đi dùng tập này.

Đối soát theo nhóm không giao nhau: **11,805 = 9,288 dòng hủy + 2,515 dòng giá 0 + 2 dòng giá âm**. Trong CSV này, các dòng số lượng âm đã nằm trong hai nhóm đầu.

Bộ lọc **chưa loại dòng trùng, chưa yêu cầu CustomerID/Description đầy đủ, chưa loại phí và chưa ghép hóa đơn bán với hóa đơn hoàn/hủy**. Vì vậy `df_clean` không phải `data/curated/RFM.parquet` của pipeline. Tên Revenue trong các bảng và biểu đồ của notebook luôn được hiểu theo phạm vi lọc này.

Muốn giữ dấu vết điều chỉnh, dùng nhánh audit/anomalies; muốn phân tích khách hàng bằng RFM, đọc [EDA_RFM_Guide.md](EDA_RFM_Guide.md). Notebook EDA này chỉ lọc trong bộ nhớ, không ghi Parquet.

## 5. Ý nghĩa các giá trị đặc biệt trong Description, StockCode và InvoiceNo

Notebook chỉ in ra một phần nhỏ dữ liệu, nên mục này kết hợp hai nguồn và ghi rõ ở cột Nguồn của mỗi bảng:

- Notebook: giá trị xuất hiện trực tiếp trong output của notebook.

- CSV: mã, mô tả và hóa đơn đã đối chiếu trực tiếp trong `data/raw/data.csv` khi chuyển guide. Bản Word trước đây gọi các dòng này là “Tham khảo”. Ý nghĩa nghiệp vụ là diễn giải từ mô tả/ngữ cảnh, không phải nhãn đã được xác nhận bởi doanh nghiệp.

### 5.1. Cột Description

Description thường là tên sản phẩm. Trong dữ liệu hiện tại, cột này còn chứa tên phí/dịch vụ, ghi chú và giá trị thiếu.

Bảng 4. Các nhóm giá trị trong cột Description

| Nhóm | Ví dụ | Ý nghĩa | Nguồn |
| --- | --- | --- | --- |
| Tên sản phẩm | WHITE HANGING HEART T-LIGHT HOLDER; REGENCY CAKESTAND 3 TIER; PARTY BUNTING | Tên hàng hóa thật, viết in hoa. Chiếm đại đa số các dòng. | Notebook |
| Phí và dịch vụ | POSTAGE; DOTCOM POSTAGE; Manual | Phí vận chuyển và dòng nhập thủ công. Có doanh thu nhưng không phải hàng hóa. | Notebook |
| Phí và dịch vụ (khác) | CARRIAGE; Discount; Bank Charges; AMAZON FEE; SAMPLES; CRUK Commission; Adjust bad debt | Phí vận chuyển, chiết khấu, phí ngân hàng, phí sàn Amazon, hàng mẫu, hoa hồng, điều chỉnh nợ xấu. | CSV |
| Ghi chú kho | check; lost; missing; smashed | Ghi chú nội bộ viết chữ thường, thay cho tên sản phẩm. Trong output đều đi kèm Quantity âm, UnitPrice = 0 và không có CustomerID. | Notebook |
| Ghi chú kho (khác) | damaged; damages; wet damaged; thrown away; found; adjustment; ? | Các ghi chú cùng loại thường gặp trong bộ dữ liệu này. | CSV |
| Trống (NaN) | 1,454 dòng (0.27%) | Không có mô tả. Các dòng thấy trong output đều có UnitPrice = 0 và không có CustomerID. | Notebook |

Bộ dữ liệu không có từ điển chính thức cho các ghi chú kho. Ý nghĩa dưới đây là diễn giải theo nghĩa tiếng Anh và ngữ cảnh (số lượng âm, đơn giá bằng 0):

- check: hàng được đánh dấu để kiểm tra, đối chiếu lại tồn kho.

- lost, missing: hàng thất lạc hoặc thiếu so với sổ sách.

- smashed, damaged, wet damaged: hàng vỡ, hư hỏng, bị ướt.

- thrown away: hàng bị loại bỏ.

- found: tìm thấy lại hàng, thường là điều chỉnh tăng tồn kho.

- adjustment, ?: điều chỉnh sổ sách không ghi rõ lý do.

Ba điểm cần lưu ý khi dùng cột này:

- Mã và mô tả không tương ứng 1–1. Dữ liệu có 4,223 mô tả nhưng chỉ 4,070 mã, nên một số mã mang nhiều mô tả (sản phẩm đổi tên, hoặc ghi chú kho ghi vào chỗ tên sản phẩm). Ngược lại, mô tả WHITE HANGING HEART T-LIGHT HOLDER có 2,369 dòng trong khi mã 85123A chỉ có 2,313 dòng, tức mô tả này còn gắn với mã khác. Khi gom nhóm sản phẩm nên dùng StockCode làm khóa.

- Chuỗi chưa được chuẩn hóa. Output có các mô tả như "RED WOOLLY HOTTIE WHITE HEART." (dấu chấm cuối) và "PAPER CRAFT , LITTLE BIRDIE" (khoảng trắng trước dấu phẩy). Nên strip và chuẩn hóa trước khi so khớp chuỗi.

- Bộ lọc của notebook đã xử lý được nhóm ghi chú kho. Các dòng ghi chú kho và dòng trống thấy trong output đều có UnitPrice = 0 nên bị loại khỏi df_clean. Nhóm phí và dịch vụ thì vẫn còn.

### 5.2. Cột StockCode

StockCode thường có 5 chữ số; cũng có mã kèm chữ cái và mã chữ đặc biệt. Định dạng mã không đủ để xác nhận đó là hàng hóa hay phí.

Bảng 5. Các dạng StockCode và trường hợp đặc biệt

| StockCode | Description đi kèm | Ý nghĩa | Nguồn |
| --- | --- | --- | --- |
| 5 chữ số | Ví dụ 71053, 22423 | Dạng chuẩn: mỗi sản phẩm một mã. | Notebook |
| 5 chữ số + chữ cái | Ví dụ 85123A, 84406B, 84029G, 84029E | Biến thể của cùng một dòng sản phẩm (màu, họa tiết). 84029G và 84029E là hai mẫu túi chườm nóng khác nhau. | Notebook |
| POST | POSTAGE | Phí bưu điện tính cho khách. Sau làm sạch: 1,126 đơn, 78,101.88 GBP. | Notebook |
| DOT | DOTCOM POSTAGE | Phí vận chuyển của kênh bán hàng dotcom (website). 706 đơn, 206,248.77 GBP, đứng đầu bảng doanh thu. | Notebook |
| M | Manual | Dòng nhập thủ công, không gắn với sản phẩm cụ thể. 289 đơn, 78,110.27 GBP. | Notebook |
| m | Manual | Biến thể chữ thường của M. | CSV |
| D | Discount | Chiết khấu, thường ghi số lượng âm trên hóa đơn có tiền tố C. | CSV |
| C2 | CARRIAGE | Phí vận chuyển (carriage). | CSV |
| S | SAMPLES | Hàng mẫu. | CSV |
| B | Adjust bad debt | Điều chỉnh nợ xấu; đi cùng hóa đơn có tiền tố A. | CSV |
| BANK CHARGES | Bank Charges | Phí ngân hàng. | CSV |
| AMAZONFEE | AMAZON FEE | Phí trả cho sàn Amazon. | CSV |
| CRUK | CRUK Commission | Khoản hoa hồng theo mô tả; dữ liệu không xác nhận danh tính đơn vị CRUK. | CSV |
| PADS | PADS TO MATCH ALL CUSHIONS | Mô tả gợi ý ruột gối đi kèm; không suy loại hàng chỉ từ mã chữ. Quy tắc RFM hiện tại của dự án loại mã này. | CSV |
| gift_0001_10 đến gift_0001_50 | Dotcomgiftshop Gift Voucher | Phiếu quà tặng mệnh giá 10–50 GBP. | CSV |
| DCGS… | Nhiều mô tả khác nhau | Nhóm mã riêng của kênh Dotcom Gift Shop, ví dụ DCGS0003, DCGSSBOY, DCGSSGIRL. | CSV |

- DOT, POST và M nằm trong top 10 doanh thu ở mục 9 (cộng lại khoảng 362,461 GBP), nên cần tách ra trước khi xếp hạng sản phẩm hay làm market basket analysis.

- Biểu thức `^\d{5}[A-Za-z]*$` giúp liệt kê mã có dạng phổ biến và phần còn lại để xem xét. Không dùng regex này như bộ lọc hàng hóa hoàn chỉnh: mã chữ có thể có mô tả sản phẩm, còn phí cũng có thể mang mã số. Bộ lọc notebook hiện chưa loại mã theo regex.

### 5.3. Cột InvoiceNo

InvoiceNo thường là mã 6 chữ số. Một hóa đơn gồm nhiều dòng: 541,909 dòng trên 25,900 hóa đơn, trung bình khoảng 21 dòng mỗi hóa đơn. Có ba dạng mã trong dữ liệu:

- Số thuần 6 chữ số (từ 536365 đến 581xxx): hóa đơn thông thường, số tăng dần theo thời gian. Nhóm này vẫn chứa 1,336 dòng điều chỉnh tồn kho có Quantity âm và UnitPrice = 0. (Notebook)

- Tiền tố C: hóa đơn hủy hoặc hoàn hàng. Có 9,288 dòng (1.71%), tất cả mang Quantity âm. (Notebook)

- Tiền tố A: bút toán điều chỉnh nợ xấu, gồm ba hóa đơn A563185, A563186, A563187 với StockCode B. Hai trong số đó có UnitPrice = −11,062.06, khớp với 2 dòng UnitPrice < 0 và giá trị nhỏ nhất mà notebook in ra. (CSV)

Bảng 6. Một số hóa đơn đáng chú ý

| InvoiceNo | Thời điểm | Nội dung | Vì sao đáng chú ý | Nguồn |
| --- | --- | --- | --- | --- |
| 536365 | 01/12/2010 08:26 | Khách 17850 (UK); gồm 85123A, 71053, 84406B, 84029G, 84029E | Hóa đơn đầu tiên trong dữ liệu | Notebook |
| 536589 | 01/12/2010 16:50 | Mã 21777, số lượng −10, đơn giá 0, không mô tả | Điều chỉnh tồn kho không ghi chú và không có tiền tố C | Notebook |
| 573585 | 31/10/2011 14:41 | 1,114 dòng | Hóa đơn nhiều dòng nhất | Notebook |
| 581210, 581213 | 07/12/2011 18:36–18:38 | "check", số lượng −26 và −30, đơn giá 0 | Ghi chú kiểm tra tồn kho | Notebook |
| 581212 | 07/12/2011 18:38 | Mã 22578, "lost", số lượng −1,050 | Mất hàng số lượng lớn | Notebook |
| 581226 | 08/12/2011 09:56 | Mã 23090, "missing", số lượng −338 | Thiếu hàng so với sổ sách | Notebook |
| 581422 | 08/12/2011 15:24 | Mã 23169, "smashed", số lượng −235 | Hàng vỡ | Notebook |
| 541431 | 18/01/2011 10:01 | 74,215 × MEDIUM CERAMIC TOP STORAGE JAR, khách 12346, 77,183.60 GBP | Đơn có số lượng lớn thứ hai; bị hủy ngay sau đó bằng C541433 | Notebook + CSV |
| 581483 | 09/12/2011 09:15 | 80,995 × PAPER CRAFT , LITTLE BIRDIE, khách 16446, 168,469.60 GBP | Đơn có số lượng lớn nhất; bị hủy ngay sau đó bằng C581484 | Notebook + CSV |
| A563185 đến A563187 | 12/08/2011 | StockCode B, Adjust bad debt, ±11,062.06 GBP | Bút toán kế toán, không phải bán hàng | CSV |
| 537632 | 12/2010 | AMAZONFEE, đơn giá 13,541.33 | Khoản phí nhưng không bị bộ lọc loại; là UnitPrice lớn nhất trong df_clean | CSV |
| C556445 | 06/2011 | Manual, đơn giá 38,970 | UnitPrice lớn nhất của dữ liệu gốc | CSV |

Với hai đơn 541431 và 581483, notebook cho thấy thời điểm, khách hàng và giá trị (qua bảng khách hàng và bảng sản phẩm). Đối chiếu CSV thấy các dòng `C541433` và `C581484` có cùng CustomerID, StockCode, UnitPrice, số lượng đối dấu, lần lượt sau 16 và 12 phút. Đây là bằng chứng về cặp bán/hoàn; CSV không có khóa liên kết trực tiếp từ đơn hủy về đơn gốc.

Đối chiếu với bộ lọc tạo df_clean (không hủy, Quantity > 0, UnitPrice > 0):

- Loại được: hóa đơn tiền tố C, các dòng ghi chú kho, và hai dòng tiền tố A có đơn giá âm.

- Không loại được: dòng bán gốc của những đơn đã bị hủy sau đó (541431, 581483), các dòng phí và dịch vụ có giá dương (POST, DOT, M, AMAZONFEE, C2), và dòng A563185 có đơn giá dương.

### 5.4. Đoạn mã kiểm tra trên data.csv

Chạy trên df gốc (sau khi đọc file) để tái kiểm tra các trường hợp đặc biệt. Đây là đoạn mã bổ sung trong guide, không phải cell có sẵn của notebook. Cách viết hoa chỉ giúp tìm ứng viên cần đọc thêm, không phân loại được nghiệp vụ.

```python
# 1) StockCode không theo dạng chuẩn (5 chữ số + chữ cái tùy chọn)
special = df[~df["StockCode"].astype(str).str.match(r"^\d{5}[A-Za-z]*$")]
print(special.groupby(["StockCode", "Description"], dropna=False).size())

# 2) Description không viết hoa toàn bộ: ghi chú kho, phí, dịch vụ
desc = df["Description"].dropna()
print(desc[desc != desc.str.upper()].value_counts().head(40))

# 3) Tiền tố chữ của InvoiceNo (C, A, ...)
print(df["InvoiceNo"].astype(str).str.extract(r"^([A-Za-z]+)")[0].value_counts())

# 4) Các hóa đơn cụ thể trong Bảng 6
ids = ["541431", "C541433", "581483", "C581484", "537632", "C556445",
       "A563185", "A563186", "A563187"]
print(df[df["InvoiceNo"].astype(str).isin(ids)])
```

## 6. Phân phối các biến số

Đây là **mục 4 của notebook**, gồm histogram, boxplot và bảng `describe`. Mỗi quan sát là một dòng của `df_clean`, không phải một đơn hoặc một khách hàng.

### 6.1. Histogram — phần thiếu trong bản Word

Xem bộ ba histogram ở mục 4 của notebook: cell dùng `sns.histplot` với 50 bins, `kde=False`, `log_scale=True`.

Trục x biểu diễn giá trị theo thang log: khoảng cách từ 1 đến 10 bằng từ 10 đến 100. Trục y là **số dòng trong mỗi khoảng**, không phải tỷ lệ hay tổng doanh thu. Các khoảng trống ở Quantity còn do số lượng là số nguyên, trong khi bins chia theo thang log.

Phần lớn số lượng nằm ở mức nhỏ, giá và giá trị dòng tập trung ở vùng thấp hơn nhiều so với cực đại. Thang log giúp nhìn phần đông quan sát và phần đuôi trên cùng hình; không có nghĩa notebook đã thay giá trị trong `df_clean` bằng log, hoặc đã loại ngoại lệ. Cả ba biến đều dương sau bộ lọc nên có thể vẽ trên trục log.

Bản Word cũ ghi histogram gần như trống. Output đang lưu trong notebook đã hiển thị rõ các cột; phần giải thích ở đây dựa trên output hiện tại.

### 6.2. Bảng phân vị và thống kê mô tả

Bảng 7. Thống kê mô tả trên df_clean (530,104 dòng)

| Thống kê | Quantity | UnitPrice | Revenue |
| --- | --- | --- | --- |
| Trung bình | 10.54 | 3.91 | 20.12 |
| Độ lệch chuẩn | 155.52 | 35.92 | 270.36 |
| Nhỏ nhất | 1 | 0.001 | 0.001 |
| Phân vị 1% | 1 | 0.29 | 0.55 |
| Phân vị 25% | 1 | 1.25 | 3.75 |
| Trung vị (50%) | 3 | 2.08 | 9.90 |
| Phân vị 75% | 10 | 4.13 | 17.70 |
| Phân vị 95% | 30 | 9.95 | 59.70 |
| Phân vị 99% | 100 | 16.98 | 183.60 |
| Lớn nhất | 80,995 | 13,541.33 | 168,469.60 |

Phân vị 95% của Revenue bằng 59.70 nghĩa là khoảng 95% **dòng** có giá trị không quá 59.70 GBP. Không có nghĩa 95% tổng doanh thu nằm dưới mức này. Mean 20.12 cao hơn median 9.90 cho thấy các giá trị lớn kéo trung bình lên.

### 6.3. Boxplot và cách hiểu ngoại lệ

Xem bộ ba boxplot Quantity, UnitPrice và Revenue ngay sau histogram ở mục 4 của notebook; trục y dùng thang log.

Hộp thể hiện Q1–Q3, vạch giữa là median. Theo thiết lập mặc định của lệnh boxplot đang dùng, râu kéo tới quan sát xa nhất còn trong khoảng Q1 − 1.5 × IQR đến Q3 + 1.5 × IQR, với IQR = Q3 − Q1; điểm ngoài râu được vẽ riêng. Thống kê tính trên giá trị gốc rồi trục y được đặt log. Điểm ngoài râu chỉ là ngoại lệ thống kê trong phân phối gộp, chưa phải lỗi hoặc gian lận và không phải ngưỡng của job anomalies.

- Cả ba biến đều lệch phải rất mạnh: phần lớn giao dịch có giá trị nhỏ, nhưng một số ít có số lượng hoặc giá trị cực lớn (ví dụ mua sỉ).

- Một nửa số dòng có số lượng không quá 3 và doanh thu không quá 9.90 GBP, trong khi giá trị lớn nhất gấp khoảng 800–900 lần phân vị 99%.

- Notebook đề xuất cân nhắc log-transform hoặc ngưỡng phân vị 1%–99% khi mô hình hóa, nhưng chưa thực hiện hai thao tác này. Cần xem hóa đơn, sản phẩm và hoàn/hủy trước khi quyết định loại giá trị lớn; không mặc định cắt 1%–99% là quy tắc cleaning.

## 7. Phân tích theo thời gian

### 7.1. Doanh thu theo ngày và tháng

Mục 5 của notebook lần lượt vẽ đường doanh thu theo ngày, rồi biểu đồ kết hợp doanh thu và số đơn hàng theo tháng. Biểu đồ tháng dùng hai trục y: cột là Revenue, đường là số InvoiceNo duy nhất; cần đọc đúng đơn vị của từng trục.

Biểu đồ tháng gắn mỗi cột vào ngày cuối tháng nên cột nằm lệch về phía nhãn tháng kế tiếp: cột đầu tiên là tháng 12/2010, cột cao nhất là tháng 11/2011, cột cuối là tháng 12/2011.

- Doanh thu tăng dần về cuối năm, phù hợp với giả thuyết mùa mua sắm lễ hội. Khoảng quan sát chỉ hơn một năm nên chưa chứng minh mẫu mùa vụ lặp lại qua nhiều năm. Đọc từ biểu đồ, doanh thu dao động khoảng 0.5–0.8 triệu GBP/tháng từ 12/2010 đến 8/2011, vượt 1 triệu từ tháng 9 và đạt đỉnh khoảng 1.5 triệu vào tháng 11/2011 với khoảng 2,750 đơn.

- Tháng 12/2011 thấp vì dữ liệu chỉ kéo dài đến ngày 09/12.

- Chuỗi theo ngày có một khoảng trống doanh thu bằng 0 từ cuối tháng 12/2010 đến đầu tháng 1/2011, và một đỉnh khoảng 200,000 GBP vào ngày cuối cùng của dữ liệu.

### 7.2. Doanh thu theo giờ và ngày trong tuần

Hai biểu đồ cột cuối mục 5 của notebook cộng Revenue theo giờ và thứ trong toàn bộ giai đoạn, không phải doanh thu trung bình mỗi giờ hay mỗi ngày.

- Không có giao dịch vào Thứ Bảy trong mẫu đang phân tích; dữ liệu không xác nhận nguyên nhân hay lịch hoạt động chính thức. Cần lưu ý khi resample theo ngày hoặc tuần cho mô hình chuỗi thời gian.

- Thứ Ba và Thứ Năm có doanh thu cao nhất (tổng cả giai đoạn khoảng 2.2 triệu GBP mỗi thứ); Chủ Nhật thấp nhất (khoảng 0.8 triệu).

- Giao dịch diễn ra từ 6h đến 20h, tập trung trong khung 10h–15h, cao nhất ở 10h và 12h.

## 8. Phân tích theo quốc gia

Bảng 8. Top 10 quốc gia theo doanh thu

| # | Quốc gia | Doanh thu (GBP) | Tỷ trọng | Số đơn | Số khách hàng |
| --- | --- | --- | --- | --- | --- |
| 1 | United Kingdom | 9,025,222.08 | 84.6% | 18,019 | 3,920 |
| 2 | Netherlands | 285,446.34 | 2.7% | 94 | 9 |
| 3 | EIRE | 283,453.96 | 2.7% | 288 | 3 |
| 4 | Germany | 228,867.14 | 2.1% | 457 | 94 |
| 5 | France | 209,715.11 | 2.0% | 392 | 87 |
| 6 | Australia | 138,521.31 | 1.3% | 57 | 9 |
| 7 | Spain | 61,577.11 | 0.6% | 90 | 30 |
| 8 | Switzerland | 57,089.90 | 0.5% | 54 | 21 |
| 9 | Belgium | 41,196.34 | 0.4% | 98 | 25 |
| 10 | Sweden | 38,378.33 | 0.4% | 36 | 8 |

Tỷ trọng của các nước ngoài UK được tính lại từ tổng doanh thu ≈ 10.67 triệu GBP; notebook chỉ in tỷ trọng của UK.

Biểu đồ thanh ngang ở mục 6 của notebook minh họa top 10 quốc gia trong bảng trên. Độ dài thanh biểu diễn tổng Revenue, không phải số đơn hoặc số khách.

- Doanh thu tập trung áp đảo tại United Kingdom (84.6%); mỗi quốc gia còn lại chiếm dưới 3%.

- Netherlands, EIRE và Australia có doanh thu lớn dù rất ít khách hàng (3–9 khách), tức phụ thuộc vào một vài khách mua lớn.

- Khi so sánh giữa các quốc gia cần lưu ý sự mất cân bằng dữ liệu rất lớn giữa UK và phần còn lại.

## 9. Phân tích theo sản phẩm

Bảng 9. Top 10 sản phẩm theo doanh thu

| # | Mã | Mô tả | Số lượng | Doanh thu (GBP) | Số đơn |
| --- | --- | --- | --- | --- | --- |
| 1 | DOT | DOTCOM POSTAGE | 706 | 206,248.77 | 706 |
| 2 | 22423 | REGENCY CAKESTAND 3 TIER | 13,879 | 174,484.74 | 1,988 |
| 3 | 23843 | PAPER CRAFT , LITTLE BIRDIE | 80,995 | 168,469.60 | 1 |
| 4 | 85123A | WHITE HANGING HEART T-LIGHT HOLDER | 37,599 | 104,340.29 | 2,189 |
| 5 | 47566 | PARTY BUNTING | 18,295 | 99,504.33 | 1,685 |
| 6 | 85099B | JUMBO BAG RED RETROSPOT | 48,474 | 94,340.05 | 2,089 |
| 7 | 23166 | MEDIUM CERAMIC TOP STORAGE JAR | 78,033 | 81,700.92 | 247 |
| 8 | M | Manual | 7,224 | 78,110.27 | 289 |
| 9 | POST | POSTAGE | 3,150 | 78,101.88 | 1,126 |
| 10 | 23084 | RABBIT NIGHT LIGHT | 30,788 | 66,964.99 | 994 |

Biểu đồ thanh ngang ở mục 7 của notebook dùng top 10 nhóm `(StockCode, Description)` theo Revenue. Nhãn mô tả bị rút gọn còn 30 ký tự; xem bảng để xác định đầy đủ sản phẩm hoặc dòng phí.

- Ba mục trong top 10 không phải sản phẩm: DOT và POST là phí vận chuyển, M là điều chỉnh thủ công. Cộng lại khoảng 362,461 GBP.

- PAPER CRAFT , LITTLE BIRDIE đứng thứ ba chỉ nhờ một đơn duy nhất 80,995 đơn vị.

- Trong bảng top 10 này, sản phẩm có giá trị bán cao nhất là REGENCY CAKESTAND 3 TIER; WHITE HANGING HEART T-LIGHT HOLDER xuất hiện trong nhiều đơn nhất (2,189 đơn). Đây là xếp hạng trên dòng được giữ lại, chưa đối trừ hoàn/hủy.

## 10. Phân tích theo khách hàng

Sau khi bỏ các dòng thiếu CustomerID, còn 4,338 khách hàng duy nhất. Với mỗi khách, notebook tính tổng doanh thu, số đơn, lần mua đầu, lần mua cuối và giá trị đơn trung bình (AOV).

`Orders` đếm InvoiceNo **khác nhau**, `AvgOrderValue = Revenue / Orders`. FirstPurchase và LastPurchase chỉ là lần đầu/cuối **quan sát được trong dữ liệu đã lọc**. Top 20% được lấy bằng `int(4,338 * 0.2) = 867` khách; tỷ lệ 74.6% chia cho tổng Revenue của các khách có ID, không chia cho tổng 10.67 triệu GBP bao gồm cả giao dịch thiếu ID. Đây là mức tập trung thực nghiệm, không phải kiểm định quy luật 80/20.

Bảng 10. Top 10 khách hàng theo doanh thu

| CustomerID | Doanh thu (GBP) | Số đơn | AOV (GBP) | Lần mua đầu | Lần mua cuối |
| --- | --- | --- | --- | --- | --- |
| 14646 | 280,206.02 | 73 | 3,838.44 | 20/12/2010 | 08/12/2011 |
| 18102 | 259,657.30 | 60 | 4,327.62 | 07/12/2010 | 09/12/2011 |
| 17450 | 194,550.79 | 46 | 4,229.37 | 07/12/2010 | 01/12/2011 |
| 16446 | 168,472.50 | 2 | 84,236.25 | 18/05/2011 | 09/12/2011 |
| 14911 | 143,825.06 | 201 | 715.55 | 01/12/2010 | 08/12/2011 |
| 12415 | 124,914.53 | 21 | 5,948.31 | 06/01/2011 | 15/11/2011 |
| 14156 | 117,379.63 | 55 | 2,134.18 | 03/12/2010 | 30/11/2011 |
| 17511 | 91,062.38 | 31 | 2,937.50 | 01/12/2010 | 07/12/2011 |
| 16029 | 81,024.84 | 63 | 1,286.11 | 01/12/2010 | 01/11/2011 |
| 12346 | 77,183.60 | 1 | 77,183.60 | 18/01/2011 | 18/01/2011 |

Histogram ở mục 8 của notebook vẽ tổng Revenue của từng khách trên trục x dạng log. Trục y đếm khách hàng, khác với histogram ở mục 4 đếm dòng hóa đơn.

- Top 20% khách hàng đóng góp 74.6% tổng doanh thu, gần với nguyên lý Pareto 80/20.

- Trên thang log, phân phối doanh thu theo khách hàng gần hình chuông, tập trung quanh vài trăm đến khoảng 1,000 GBP, với đuôi phải kéo dài tới hơn 100,000 GBP.

- Hai khách trong top 10 (16446 và 12346) có mặt chỉ nhờ 1–2 đơn cực lớn, khác hẳn nhóm mua thường xuyên như 14911 với 201 đơn.

- Cấu trúc này phù hợp để triển khai phân khúc RFM (Recency, Frequency, Monetary).

## 11. Tổng kết insight chính

- Chất lượng dữ liệu: khoảng 25% dòng thiếu CustomerID; có 9,288 dòng giao dịch hủy cần loại khi phân tích doanh số bán ra thực tế.

- Phân phối lệch phải mạnh ở Quantity, UnitPrice, Revenue; cân nhắc log-transform hoặc xử lý outlier khi mô hình hóa.

- Dấu hiệu mùa vụ: doanh thu tăng mạnh về cuối năm; không có giao dịch vào Thứ Bảy trong mẫu. Cần lưu ý phạm vi quan sát khi xây dựng chuỗi thời gian cho dự báo.

- Tập trung địa lý: United Kingdom chiếm 84.6% doanh thu; các thị trường khác có dữ liệu thưa.

- Tập trung khách hàng: top 20% khách hàng tạo ra 74.6% doanh thu, là cơ sở cho phân khúc RFM/CLV.

### Hướng phân tích tiếp theo notebook gợi ý

- Xây dựng chuỗi thời gian doanh thu theo ngày hoặc tuần để dự báo (SARIMA, SARIMAX, Prophet).

- Phân khúc khách hàng bằng RFM và clustering (K-Means).

- Market basket analysis (association rules) để tìm các sản phẩm hay được mua kèm nhau.

## 12. Một số lưu ý khi sử dụng kết quả

Các điểm dưới đây rút ra khi đối chiếu phần nhận xét với output trong notebook, không nằm trong phần tổng kết gốc.

- Nhận xét ở mục làm sạch viết rằng gần như toàn bộ dòng Quantity < 0 là giao dịch hủy. Theo bảng chéo, con số là 9,288 trên 10,624 dòng (87.4%); 1,336 dòng còn lại có ngữ cảnh gợi ý điều chỉnh tồn kho.

- Mục 6 giải thích đủ histogram, boxplot và bảng phân vị của mục 4 notebook; xem hình trực tiếp trong notebook. Thang log là cách hiển thị; notebook không tự loại các điểm ngoài râu boxplot.

- df_clean vẫn chứa các mã phi sản phẩm (DOT, POST, M; xem mục 5.2). Nên loại chúng trước khi xếp hạng sản phẩm hoặc làm market basket analysis.

- Dữ liệu gốc có Quantity nhỏ nhất là −80,995, đối xứng với đơn 80,995 đơn vị của sản phẩm 23843. Điều này gợi ý đơn đó đã bị hủy, nhưng dòng bán ban đầu vẫn nằm trong df_clean vì bộ lọc chỉ bỏ dòng hủy chứ không bỏ đơn gốc tương ứng. Đỉnh doanh thu ngày 09/12/2011, vị trí thứ 3 của sản phẩm này và vị trí thứ 4 của khách hàng 16446 đều chịu ảnh hưởng, nên cần kiểm tra lại trước khi dùng cho dự báo hay RFM.

- Tháng 12/2011 chỉ có 9 ngày dữ liệu, không nên so sánh trực tiếp với các tháng đủ.

- `CustomerID` được pandas suy luận thành float trong notebook này, nhưng bản chất là mã định danh. Không diễn giải trung bình hoặc độ lệch chuẩn CustomerID thành hành vi khách hàng. Schema trong guide mô tả đúng notebook này, không thay thế schema của pipeline Spark.

- Với `groupby(["StockCode", "Description"])`, một mã có nhiều mô tả có thể bị tách thành nhiều nhóm; giá trị thiếu ở khóa nhóm mặc định bị bỏ. `Customers = nunique(CustomerID)` không tính khách thiếu ID, dù Revenue của quốc gia vẫn gồm các dòng thiếu ID.

- Đọc tiếp [EDA_Anomalies_Guide_VI.md](EDA_Anomalies_Guide_VI.md) để xem điều kiện áp dụng và cách đánh giá từng cờ. Kết quả trên CSV thô và trên Parquet đã loại trùng có mẫu số khác nhau.
