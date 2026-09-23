Notebook trả lời câu hỏi: **nhóm giao dịch thiếu CustomerID phân tích được những gì, và dấu hiệu nào cần được xem xét thêm?**

Luồng đọc:

**A: kiểm tra nguồn → B: so sánh nhóm khách hàng → C: hiểu mô tả/nghiệp vụ → D: kiểm tra đủ thông tin → E: chọn ứng viên → F: xem ngữ cảnh → G: rút ra đề xuất.**

Ba khái niệm cần phân biệt:

| Khái niệm | Ví dụ | Điều chưa thể kết luận |
|---|---|---|
| Chất lượng dữ liệu | Thiếu CustomerID | Không có nghĩa giao dịch sai |
| Nghiệp vụ đặc biệt | Hủy/hoàn, phí, điều chỉnh kho | Không tự động là bất thường cần loại |
| Khác biệt thống kê | Số lượng vượt ngưỡng cùng sản phẩm | Không chứng minh lỗi hay gian lận |

Mỗi dòng dữ liệu là **một dòng hóa đơn**, không phải một hóa đơn hay một khách hàng. Một hóa đơn có thể có nhiều dòng; các cờ có thể cùng xuất hiện trên một dòng.

## A. Nguồn dữ liệu, schema và đối chiếu

### Mục đích

Trước khi giải thích bất thường, cần biết đang phân tích dữ liệu nào, có giữ đúng nội dung nguồn không và có thể tái lập kết quả không.

Notebook đọc Parquet bằng **PyArrow**, sau đó dùng pandas để thống kê. CSV gốc được đọc riêng để đối chiếu. Nó không chạy lại cleaning, không sửa CSV và không ghi đè Parquet.

### Các output có ý nghĩa gì?

- **Thời điểm và phiên bản thư viện:** giúp biết kết quả được tạo trong môi trường nào.
- **Schema:** xác nhận kiểu cột; mã hóa đơn/khách hàng là chuỗi, ngày là timestamp, số lượng là số nguyên, đơn giá là số thực.
- **SHA-256:** dấu vân tay nội dung từng file đầu vào. So sánh trước/sau giúp xác nhận notebook không làm thay đổi dữ liệu; không chứng minh nội dung dữ liệu đúng về nghiệp vụ.
- **Bảng null/rỗng/số không hữu hạn:** kiểm tra thiếu dữ liệu và giá trị như vô cực. Chuỗi rỗng hoặc chỉ có khoảng trắng cũng được xét là thiếu trong các điều kiện liên quan.
- **Bảng đối chiếu:** phân biệt dữ liệu trước và sau loại trùng.

| Nguồn | Số dòng | Dòng thiếu CustomerID | Tỷ lệ |
|---|---:|---:|---:|
| CSV gốc | 541.909 | 135.080 | 24,93% |
| CSV sau loại trùng | 536.641 | 135.037 | 25,16% |
| Parquet anomalies | 536.641 | 135.037 | 25,16% |

CSV có **5.268 dòng trùng toàn bộ cột**. Tỷ lệ thiếu ID tăng nhẹ sau loại trùng không có nghĩa phát sinh thêm giá trị thiếu: tổng mẫu số giảm, trong khi số dòng thiếu ID chỉ giảm 43.

### Khớp số dòng chưa đồng nghĩa khớp nội dung

Notebook so sánh cả nội dung và số lần xuất hiện của các tổ hợp cột, không chỉ so số dòng.

Kết quả có **2.540 tổ hợp dòng khác nhau** khi xét đủ tám cột. Khi bỏ riêng Description, bảy cột còn lại khớp cả nội dung và số lần xuất hiện. Các ví dụ cho thấy khác biệt dấu ngoặc kép trong Description, gợi ý khác cách đọc CSV.

**2.540 là số tổ hợp khác nhau trong phép đối chiếu hai phía, không nên diễn giải thành 2.540 giao dịch sai.** Một bản ghi có hai phiên bản Description có thể đóng góp một tổ hợp mỗi phía. Các ví dụ ghép chi tiết chỉ dùng khóa bảy cột duy nhất để tránh nhân bản dòng.

Ý nghĩa thực tế: các kết luận chính áp dụng cho Parquet đang đọc. Chưa nên tuyên bố Parquet hoàn toàn giống CSV hoặc dùng hai nguồn thay thế nhau khi phân tích mô tả.

## B. So sánh nhóm có và thiếu CustomerID

### Mục đích

Tìm hiểu nhóm thiếu ID có đặc điểm gì thay vì mặc định đó là dữ liệu bỏ đi. Notebook chia thành hai nhóm không giao nhau và bao phủ toàn bộ dữ liệu.

| Nhóm | Số dòng | Số hóa đơn |
|---|---:|---:|
| Có ID | 401.604 | 22.190 |
| Thiếu ID | 135.037 | 3.710 |

Trong dữ liệu hiện tại, các hóa đơn thiếu ID đều thiếu trên toàn bộ dòng; không có hóa đơn hỗn hợp có/thiếu ID và không có hóa đơn chứa nhiều ID khác nhau.

**Vì vậy không thể bổ sung ID đơn giản bằng cách lấy ID từ dòng khác của cùng hóa đơn.** Kết quả cũng chưa chứng minh nhóm này là khách vãng lai hoặc bán tại quầy.

### Đọc các tỷ lệ và phân vị

Bảng so sánh gồm tỷ lệ hủy, số lượng âm, giá 0, giá âm, mức độ thiếu các trường và phân vị của số lượng/giá/giá trị dòng.

Tỷ lệ của mỗi nhóm dùng **số dòng trong chính nhóm đó** làm mẫu số. Ví dụ, hủy chiếm khoảng 0,28% nhóm thiếu ID và 2,21% nhóm có ID; đây là tỷ lệ dòng, không phải tỷ lệ khách hàng hủy.

Phân vị 95% là mức mà khoảng 95% quan sát nằm ở hoặc dưới đó. Phân vị giúp hiểu phân bố; notebook chưa coi mọi dòng vượt phân vị 95% là bất thường.

### Phân biệt hai loại tổng giá trị

| Chỉ tiêu | Cách hiểu |
|---|---|
| Giá trị có dấu | Tổng Quantity × UnitPrice của các dòng tính được, bao gồm giá trị âm/dương |
| Giá trị dòng dương | Tổng của các dòng có số lượng và đơn giá dương |

Nhóm thiếu ID có tổng giá trị có dấu khoảng **1.447.487,53**, còn tổng giá trị dòng dương khoảng **1.754.901,91**. Chưa nên gọi hai con số này là doanh thu thuần: chúng có thể chứa phí, điều chỉnh và các giao dịch cần đối soát.

### Biểu đồ tháng, quốc gia và sản phẩm

Chiều cao cột là tỷ lệ dòng trong từng nhóm ID. Biểu đồ quốc gia/sản phẩm chỉ hiển thị top 10 theo tổng số dòng; các cột hiển thị không bắt buộc cộng thành 100%.

Biểu đồ giúp đặt câu hỏi về cơ cấu thị trường, sản phẩm và thời điểm. Nó không xác nhận thiếu ID là nguyên nhân gây ra khác biệt.

## C. Đối chiếu mô tả với tên sản phẩm theo StockCode

### Tại sao không chỉ tìm từ khóa?

`HOLLY TOP CHRISTMAS STOCKING` là tên sản phẩm mã **85084**. Tìm chuỗi `STOCK` sẽ bắt nhầm `STOCKING`. Ngay cả khớp đúng từ vẫn có thể nhầm: `CHECK` có thể chỉ họa tiết ca-rô; `MOULD` có thể là khuôn làm bánh.

Vì vậy notebook dùng cả **khớp từ đầy đủ** và **tên tham chiếu của cùng StockCode**.

### Tên tham chiếu được xác định như thế nào?

Một tên được giữ làm tham chiếu nếu xuất hiện trên ít nhất **hai hóa đơn khác nhau** có số lượng và giá dương, không phải hóa đơn hủy, không thuộc phí hoặc mã chữ đặc biệt.

Mô tả được chuẩn hóa chữ hoa, khoảng trắng; giữ mọi tên đạt điều kiện của cùng mã. Hai dòng trong cùng một hóa đơn không đủ để đạt điều kiện hai hóa đơn.

Tên tham chiếu là bằng chứng từ lịch sử bán, **không phải danh mục sản phẩm chính thức**. Trong dữ liệu hiện tại, tên HOLLY TOP CHRISTMAS STOCKING của 85084 có trên 7 hóa đơn bán đủ điều kiện.

### Ý nghĩa các cờ

| Cờ | Ý nghĩa |
|---|---|
| `cancel` | InvoiceNo bắt đầu bằng C sau chuẩn hóa |
| `service` | Mã/mô tả thuộc danh sách phí, dịch vụ đã dùng trong cleaning |
| `special_code` | StockCode chuẩn hóa chỉ gồm chữ cái và khoảng trắng; là dấu hiệu hình thức mã, không xác nhận nghiệp vụ |
| `zero_negative` | UnitPrice = 0 và Quantity < 0 |
| `zero_positive` | UnitPrice = 0 và Quantity > 0 |
| `sale_shape` | Có InvoiceNo, không phải C, Quantity và UnitPrice dương, giá trị tính được |
| `keyword_product_name` | Có từ khóa nhưng mô tả trùng tên tham chiếu của cùng mã |
| `keyword_unresolved` | Có từ khóa nhưng mã chưa có tên tham chiếu đủ bằng chứng hoặc thiếu mã |
| `stock_words` | Từ khóa kiểm kê/điều chỉnh, khác tên tham chiếu của mã có tham chiếu |
| `damage_words` | Từ khóa hư hỏng theo điều kiện đối chiếu tương tự |
| `loss_words` | Từ khóa thiếu/thất lạc theo điều kiện đối chiếu tương tự |
| `coding_words` | Từ khóa ghi sai mã/nhãn theo điều kiện đối chiếu tương tự |

Các từ như CHECK, ADJUST, STOCK, FOUND gợi ý kiểm kê; DAMAGED, BROKEN, RUSTY gợi ý hư hỏng; MISSING, LOST gợi ý thất lạc; WRONG, BARCODE, CODED gợi ý vấn đề ghi nhận. Chúng vẫn chỉ là suy luận.

### Kết quả sau sửa trong nhóm thiếu ID

| Cờ | Số dòng |
|---|---:|
| sale_shape | 132.186 |
| zero_negative | 1.336 |
| zero_positive | 1.134 |
| stock_words | 246 |
| damage_words | 163 |
| loss_words | 20 |
| coding_words | 31 |
| keyword_product_name | 293 |
| keyword_unresolved | 28 |

Stock giảm từ **551 xuống 246**, damage từ **1.436 xuống 163** sau thay cả cách khớp từ lẫn đối chiếu tên. Không nên coi toàn bộ mức giảm là do riêng bước đối chiếu StockCode.

97,89% nhóm thiếu ID có hình thức bán hàng, nhưng cờ này chưa loại hết phí và chưa xác nhận hợp lệ. Ngược lại, số lượng âm/giá 0 có thể là nghiệp vụ kho hợp lệ.

### Cách đọc bảng giao nhau

Ô giao giữa hai cờ là số dòng đồng thời có cả hai cờ. Đường chéo là tổng số dòng có cờ đó. Các cờ không phải một phân loại loại trừ nhau, nên không cộng tổng từng cờ để ra số giao dịch duy nhất.

Cờ tên sản phẩm chỉ loại bỏ một lý do nghi vấn từ mô tả; nó không xóa các dấu hiệu bất thường về giá hoặc số lượng của dòng đó.

## D. Điều kiện áp dụng từng phép phân tích

### Mục đích

D trả lời **“có đủ dữ liệu để phân tích không?”**, chưa trả lời “có bất thường không?”.

| Phân tích | Điều kiện tối thiểu |
|---|---|
| Số lượng theo sản phẩm | StockCode và Quantity hữu hạn |
| Đơn giá theo sản phẩm | StockCode và UnitPrice hữu hạn |
| Giá trị dòng | Quantity × UnitPrice tính được và hữu hạn |
| Tổng hợp hóa đơn | Có InvoiceNo; kiểm tra thêm số dòng thiếu thành phần tính tiền |
| Theo thời gian | InvoiceDate hợp lệ |
| Lịch sử khách hàng | Có CustomerID |

Số âm vẫn là số hợp lệ về kiểu dữ liệu. Ví dụ Quantity = -5 có thể phân tích hoàn/hủy, dù không thuộc nhóm bán hàng dương dùng cho IQR.

| Cột output | Cách đọc |
|---|---|
| `du_dieu_kien` | Số dòng đủ thông tin cho phép phân tích |
| `khong_du` | Số dòng thiếu điều kiện |
| `mau_so` | Tổng số dòng của nhóm ID |
| `du_pct` | du_dieu_kien / mau_so × 100 |

Trong dữ liệu hiện tại, toàn bộ **135.037 dòng thiếu ID** có đủ trường tối thiểu cho các phân tích ở D, ngoại trừ lịch sử khách hàng. Điều đó không có nghĩa tất cả đều chạy được IQR: E còn yêu cầu đúng nhóm nghiệp vụ, ít nhất 30 quan sát cùng sản phẩm và IQR dương.

Ví dụ: một dòng thiếu ID, có mã hàng, ngày, số lượng 12 và giá 2,5 vẫn có thể so sánh giá/số lượng theo sản phẩm và tổng hợp vào hóa đơn. Nó không thể được gắn vào lịch sử của một khách hàng xác định.

## E. Thử quy tắc nghiệp vụ và thống kê

### E1. Quy tắc nghiệp vụ

| Quy tắc | Điều kiện đánh dấu | Số dòng hiện tại, toàn bộ dữ liệu |
|---|---|---:|
| `C_quantity_nonnegative` | Hóa đơn C nhưng Quantity ≥ 0 | 0 |
| `negative_quantity_nonC` | Quantity âm nhưng không phải hóa đơn C | 1.336 |
| `negative_price` | UnitPrice âm | 2 |
| `zero_in_paid_invoice` | Dòng giá 0 trong hóa đơn có ít nhất một dòng giá dương | 395 |
| `all_zero_invoice` | Mọi dòng trong hóa đơn đều giá 0 và đủ giá hữu hạn | 2.115 |
| `multiple_customer_ids` | Hóa đơn có nhiều CustomerID chuẩn hóa khác nhau | 0 |

Các phép kiểm tra có điều kiện áp dụng riêng. Ví dụ, thiếu giá ở một dòng khiến hóa đơn không đủ cơ sở cho phép kiểm tra “toàn giá 0”. Không có cờ khi không đủ thông tin khác với đã kiểm tra và không vi phạm.

Trong mã hiện tại, “có hàng trả tiền” được nhận diện bằng **UnitPrice > 0**; đây không phải bằng chứng thanh toán thực tế. Bộ dữ liệu không có xác nhận thu tiền cho phép kiểm tra này.

Kết quả đếm dòng: một hóa đơn toàn giá 0 có năm dòng đóng góp năm dòng bị đánh dấu. Giá 0 có thể là hàng tặng; số lượng âm không phải C có thể là điều chỉnh kho. Cờ yêu cầu đối soát, không ra lệnh xóa.

### E2. IQR theo từng sản phẩm

Nhóm tham chiếu gồm các dòng có hình thức bán hàng, có StockCode, loại phí/dịch vụ và mã chữ đặc biệt. Cả dòng có và thiếu ID đều tham gia. Các cờ mô tả ở C không tự động loại dòng khỏi nhóm IQR hiện tại.

Ba biến được xét riêng: **Quantity**, **UnitPrice**, **line_value = Quantity × UnitPrice**.

```text
Q1 = phân vị 25%
Q3 = phân vị 75%
IQR = Q3 − Q1
Ngưỡng mức 1,5 = Q3 + 1,5 × IQR
Ngưỡng mức 3   = Q3 + 3 × IQR
Đánh dấu khi giá trị > ngưỡng
```

Ví dụ minh họa, không phải ngưỡng thật của một mã hàng: Q1 = 2, Q3 = 10 thì IQR = 8; hai ngưỡng là 22 và 34. Số lượng 25 vượt mức 1,5; 40 vượt cả hai; đúng 34 không vượt mức 3.

Đây chỉ là **ngưỡng phía trên**, chưa tìm giao dịch có giá/số lượng thấp bất thường bằng IQR phía dưới.

### E3. Khi nào IQR không được áp dụng?

Sản phẩm cần ít nhất **30 dòng đủ điều kiện**, không phải 30 hóa đơn hay khách hàng. Nếu ít mẫu hoặc IQR bằng 0, notebook bỏ qua phép kiểm tra đó, không thay bằng ngưỡng chung toàn bộ sản phẩm.

Đơn giá có **281.189 dòng** thuộc nhóm đủ mẫu nhưng IQR bằng 0. Điều này có thể xảy ra khi phần lớn giao dịch có cùng một giá. IQR bằng 0 không đảm bảo mọi giá trong nhóm giống nhau; vì vậy cách bỏ qua này có thể bỏ sót ngoại lệ và cần nghiên cứu tiếp.

### E4. Cách đọc bảng kết quả

| Cột | Ý nghĩa |
|---|---|
| `mau_so` | Tổng số dòng trong nhóm có/thiếu ID |
| `du_dieu_kien` | Số dòng thực sự được áp dụng quy tắc |
| `danh_dau` | Số dòng vi phạm |
| `pct_toan_nhom` | danh_dau / mau_so × 100 |
| `pct_du_dieu_kien` | danh_dau / du_dieu_kien × 100 |

Ví dụ đơn giá, mức 3, nhóm thiếu ID: tổng nhóm **135.037**, được kiểm tra **79.651**, đánh dấu **6.924**. Tương ứng **5,13% toàn nhóm** và **8,69% số dòng được kiểm tra**.

| Tỷ lệ vượt mức 3 trên dòng đủ điều kiện | Có ID | Thiếu ID |
|---|---:|---:|
| Quantity | 6,37% | 1,03% |
| UnitPrice | 0,45% | 8,69% |
| line_value | 6,28% | 2,59% |

Nhóm thiếu ID nổi bật về đơn giá, còn nhóm có ID nổi bật hơn về số lượng và giá trị dòng. Có thể tồn tại cơ chế giá, kênh bán hoặc cơ cấu mua khác nhau; bảng này chưa xác nhận nguyên nhân.

### E5. Độ nhạy và giới hạn

Mức 3 chọn ít dòng hơn mức 1,5, nhưng không đồng nghĩa chính xác hơn khi chưa có nhãn xác thực. Một dòng có thể vi phạm nhiều biến; số dòng hợp phải đếm một lần.

Trong nhóm thiếu ID, **126.009 dòng** được ít nhất một phép IQR đánh giá; **10.102 dòng** vượt ít nhất một ngưỡng mức 3. Đây là ứng viên, không phải 10.102 giao dịch sai.

Ngưỡng được tính từ toàn kỳ dữ liệu, kể cả dữ liệu về sau so với giao dịch đang xét. Kết quả phù hợp EDA hồi cứu, chưa chứng minh khả năng phát hiện hằng ngày chỉ dùng quá khứ. Chưa đo precision/recall vì chưa có nhãn thật.

## F. Xem ví dụ và đối soát ngữ cảnh

### F1. Tại sao cần F sau E?

E chỉ nói dòng nào thỏa điều kiện. F đặt câu hỏi **“trong ngữ cảnh, điều này có lời giải thích hợp lý không?”** Một đơn mua sỉ lớn hoặc một cặp mua rồi hủy có thể vượt IQR nhưng không phải gian lận.

### F2. Cách chọn ví dụ

Mỗi quy tắc hiển thị tối đa **10 dòng**, không phải toàn bộ kết quả. Quy tắc IQR sắp theo trị tuyệt đối của biến đang xét; quy tắc nghiệp vụ sắp theo trị tuyệt đối giá trị dòng. Sau đó dùng ngày, hóa đơn, sản phẩm và các cột khác để thứ tự ổn định.

Với các dòng giá 0, giá trị dòng đều bằng 0 nên thứ tự chủ yếu do các khóa tiếp theo. Đây là mẫu có chủ đích để kiểm tra, không phải mẫu ngẫu nhiên để ước lượng chất lượng detector.

Bảng hóa đơn lớn nhất cộng giá trị có dấu theo hóa đơn rồi xếp theo trị tuyệt đối. Nó chưa áp ngưỡng bất thường hóa đơn, chưa tự bù trừ các hóa đơn mua và hủy khác mã.

### F3. Đối soát sâu năm chủ đề

Notebook chọn tối đa một ví dụ cho mỗi chủ đề: điều chỉnh tiềm năng, giá âm, giá 0 trong hóa đơn có giá dương, số lượng cực đoan thiếu ID, số lượng cực đoan có ID.

| Ngữ cảnh | Câu hỏi |
|---|---|
| Cùng hóa đơn | Các dòng còn lại giúp giải thích dòng đang xét không? |
| Cùng StockCode | Giá/số lượng khác lịch sử sản phẩm thế nào? |
| Cùng CustomerID, nếu có | Khách có lịch sử mua tương tự không? |
| Ứng viên hoàn/hủy | Có dòng cùng sản phẩm, cùng giá và lượng đối dấu không? |

Cùng hóa đơn và lịch sử khách hàng chỉ hiển thị tối đa năm dòng mẫu, cùng các thống kê tổng hợp; không phải đã xem thủ công hết tất cả dòng liên quan.

### F4. Giới hạn đối chiếu hoàn/hủy

Ứng viên phải cùng sản phẩm, cùng đơn giá, số lượng đối dấu và trạng thái C khác nhau; khi dòng đang xét có ID thì yêu cầu cùng khách hàng.

Notebook chưa xác minh liên kết hóa đơn gốc, chưa ghép một-một, chưa yêu cầu dòng hủy xảy ra sau dòng mua, chưa giải quyết hoàn một phần. Khi thiếu ID, mức độ chắc chắn còn thấp hơn.

Có một ứng viên phù hợp chỉ cung cấp bằng chứng để xem thêm. Không tìm thấy ứng viên cũng không chứng minh không có hoàn/hủy.

### F5. Đọc các nhận định

| Nhận định | Ý nghĩa thực tế |
|---|---|
| Có giải thích nghiệp vụ | Có dấu hiệu mô tả hỗ trợ giả thuyết, chưa xác nhận |
| Còn đáng nghi | Vượt ngưỡng và chưa thấy bằng chứng giải thích trong các kiểm tra hiện tại |
| Chưa đủ bằng chứng | Chưa thể kết luận sai hay bình thường |

Các câu nhận định được tạo theo logic notebook, không phải chuyên gia đã duyệt mọi giao dịch. “Có giải thích” chưa đủ để gỡ tất cả cờ; “còn đáng nghi” chưa đủ để kết luận gian lận.

## G. Kết luận và đề xuất detector

### G1. Điều được kết quả ủng hộ

- Không bỏ toàn bộ nhóm thiếu ID: phần lớn có đủ thông tin phân tích dòng, hóa đơn, sản phẩm và thời gian.
- Tách cờ chất lượng dữ liệu khỏi cờ nghiệp vụ/thống kê.
- Đối chiếu mô tả theo StockCode để tránh bắt nhầm tên sản phẩm.
- Lưu lý do một phép kiểm tra không áp dụng được; không mặc định bình thường.
- Giữ các trường hợp bị đánh dấu để đối soát, không tự loại khỏi dữ liệu gốc.

### G2. Điều chưa được chứng minh

Chưa biết nguyên nhân thiếu ID cho toàn bộ nhóm; chưa xác nhận nhóm này là khách vãng lai; chưa có số lượng giao dịch gian lận; chưa chọn ngưỡng vận hành được nghiệm thu; chưa chứng minh các ghi chú kho đều đúng cách diễn giải của từ khóa.

### G3. Đầu ra detector được đề xuất

Đây là **đề xuất**, file job detector chưa được triển khai trong phạm vi EDA.

| Trường | Vai trò |
|---|---|
| Tám cột gốc | Giữ khả năng đối soát về giao dịch nguồn |
| `data_quality_flags` | Ghi thiếu ID hoặc vấn đề dữ liệu |
| `anomaly_flags` | Ghi rõ những quy tắc bị vi phạm |
| `checks_applied` | Những phép kiểm tra đã chạy |
| `checks_not_applied` | Những phép không chạy và lý do |
| Trạng thái tổng | flagged / not_flagged / not_assessable |

- **flagged:** có ít nhất một cờ bất thường.
- **not_flagged:** đã áp dụng ít nhất một phép kiểm tra nhưng không có cờ; không đảm bảo mọi phép kiểm tra đều đã chạy hoặc giao dịch chắc chắn đúng.
- **not_assessable:** không có phép kiểm tra nào áp dụng được.

Một dòng thiếu ID vẫn có thể được kiểm tra số lượng và bị flagged. Cờ thiếu ID không tự biến thành cờ gian lận. Trạng thái tổng không thay thế chi tiết từng phép kiểm tra.

### G4. Những việc cần chốt trước khi viết detector

Chọn quy tắc để đối soát, cách xử lý nhóm ít mẫu/IQR bằng 0, tập tham chiếu quá khứ, phân nhóm phí/hoàn/điều chỉnh và cách đánh giá cờ với người hiểu nghiệp vụ. Các ngưỡng EDA hiện tại là cơ sở thảo luận, chưa phải cấu hình vận hành đã chốt.

## Kiểm chứng cuối notebook

Notebook hiện có **11 cell code** đã chạy thành công sau bản sửa mô tả. Assertions kiểm tra:

- Hai nhóm có/thiếu ID bao phủ đúng tổng số dòng.
- Thêm cờ/ghép thống kê không thay đổi số dòng hay cột gốc.
- Dòng thiếu ID vẫn được đánh giá nếu đủ trường; dòng thiếu giá trị cần thiết không bị coi là bình thường.
- Giá trị đúng ngưỡng IQR không bị đánh dấu, vượt ngưỡng mới bị đánh dấu.
- Nhóm ít mẫu hoặc IQR bằng 0 bị bỏ qua đúng điều kiện.
- STOCK không bắt STOCKING; tên sản phẩm cùng mã được bảo vệ; thiếu tham chiếu được để chưa rõ.
- SHA-256 của các file dữ liệu trước/sau không thay đổi.

Các kiểm tra này xác nhận hành vi kỹ thuật đã nêu. Chúng không xác nhận các suy luận nghiệp vụ hoặc chất lượng detector ngoài thực tế.

## Những nhầm lẫn thường gặp

| Cách hiểu dễ nhầm | Cách hiểu đúng |
|---|---|
| 25% thiếu ID nên bỏ | Thiếu khả năng định danh khách, vẫn có thể phân tích giao dịch |
| Có từ STOCK/DAMAGE là điều chỉnh kho | Cần đối chiếu tên cùng mã và ngữ cảnh |
| Đủ điều kiện ở D là chạy được IQR | E còn yêu cầu nhóm tham chiếu, số mẫu và độ phân tán |
| Không bị cờ nghĩa là bình thường | Có thể phép kiểm tra không áp dụng hoặc không bao phủ loại bất thường đó |
| Vượt mức 3 chắc chắn sai | Chỉ vượt ngưỡng thống kê đã chọn |
| Cộng số cờ ra số giao dịch bất thường | Một dòng có thể có nhiều cờ; cần đếm hợp |
| Cùng giá/lượng đối dấu là chắc chắn hoàn đơn đó | Mới là ứng viên liên kết |
| Số liệu khác EDA cũ là lỗi | Trước hết đối chiếu cohort; EDA cũ đã bỏ Description thiếu |

**Cách sử dụng tài liệu:** đọc mục tương ứng trước khi xem bảng notebook, xác định đơn vị đếm và mẫu số, rồi đọc giới hạn trước khi đưa nhận định vào báo cáo.
