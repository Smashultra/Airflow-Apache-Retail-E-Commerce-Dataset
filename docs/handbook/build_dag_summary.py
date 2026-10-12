"""Build a concise Vietnamese DAG briefing with standard-library OOXML."""
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZipFile, ZIP_DEFLATED
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'docs/TOM_TAT_HE_THONG_DAG_VI.docx'
W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
parts = []


def p(text, style='Normal'):
    parts.append(f'<w:p><w:pPr><w:pStyle w:val="{style}"/></w:pPr>'
                 f'<w:r><w:t xml:space="preserve">{escape(text)}</w:t></w:r></w:p>')


def table(headers, rows, widths):
    xml = ['<w:tbl><w:tblPr><w:tblW w:w="0" w:type="auto"/>'
           '<w:tblBorders><w:top w:val="single" w:sz="4" w:color="CCD9E2"/>'
           '<w:bottom w:val="single" w:sz="4" w:color="CCD9E2"/>'
           '<w:insideH w:val="single" w:sz="4" w:color="CCD9E2"/>'
           '</w:tblBorders><w:tblCellMar><w:top w:w="65" w:type="dxa"/>'
           '<w:bottom w:w="65" w:type="dxa"/><w:left w:w="90" w:type="dxa"/>'
           '<w:right w:w="90" w:type="dxa"/></w:tblCellMar></w:tblPr>',
           '<w:tblGrid>' + ''.join(f'<w:gridCol w:w="{v}"/>' for v in widths) + '</w:tblGrid>']
    for i, row in enumerate([headers] + rows):
        xml.append('<w:tr><w:trPr><w:cantSplit/>' + ('<w:tblHeader/>' if i == 0 else '') + '</w:trPr>')
        for text, width in zip(row, widths):
            xml.append(f'<w:tc><w:tcPr><w:tcW w:w="{width}" w:type="dxa"/>'
                       + ('<w:shd w:fill="E8F0F5"/>' if i == 0 else '')
                       + '</w:tcPr><w:p><w:pPr><w:pStyle w:val="Cell"/></w:pPr><w:r>'
                       + ('<w:rPr><w:b/></w:rPr>' if i == 0 else '')
                       + f'<w:t>{escape(text)}</w:t></w:r></w:p></w:tc>')
        xml.append('</w:tr>')
    xml.append('</w:tbl>')
    parts.append(''.join(xml))


def page():
    parts.append('<w:p><w:r><w:br w:type="page"/></w:r></w:p>')


p('HỆ THỐNG DAG AIRFLOW – PYSPARK', 'Title')
p('Bản tóm tắt trình bày • Cập nhật theo repository ngày 12/10/2026', 'Subtitle')
p('1. Mục tiêu và kiến trúc', 'Heading1')
p('Hệ thống batch tự động biến dữ liệu giao dịch bán lẻ thành dữ liệu sạch, hồ sơ RFM/phân nhóm khách hàng và kết quả kiểm tra bất thường. Airflow điều phối thứ tự, lịch chạy và trạng thái; PySpark thực hiện xử lý dữ liệu nặng.')
p('Nguồn: UCI Online Retail qua Kaggle, 541.909 dòng, từ 01/12/2010 đến 09/12/2011. Đây là dữ liệu lịch sử; lịch daily mô phỏng xử lý theo ngày nghiệp vụ, không có giao dịch mới năm 2026.')
p('Docker Compose → Airflow 3.3.1 (Scheduler, DAG processor, API/UI, LocalExecutor) + PostgreSQL metadata → Spark local[2] → Parquet và JSON manifest trên volume dùng chung. PythonOperator/Sensor chỉ xử lý metadata; SparkSubmitOperator gọi các script PySpark.')
p('2. Bốn DAG và mối liên hệ', 'Heading1')
table(['DAG', 'Vai trò / kích hoạt'], [
    ['ecommerce_etl_dag', '6 task; hai nhánh phát Asset riêng, notify công bố snapshot tổng hợp.'],
    ['retail_rfm_training_dag', '3 task; huấn luyện và kích hoạt model có phiên bản, lịch hàng tháng.'],
    ['retail_marketing_consumer_dag', '1 task; được kích hoạt khi Asset RFM được công bố.'],
    ['retail_executive_dashboard_dag', '1 task; chỉ chờ RETAIL_SNAPSHOT_ASSET đã đối soát.'],
], [3900, 5700])
p('3. Luồng daily: sáu task bắt buộc', 'Heading1')
p('start_pipeline → validate_raw_data → submit_pyspark_etl → [compute_rfm_metrics | detect_anomalies] → notify_completion', 'Flow')
table(['Task / operator', 'Điểm cần nói'], [
    ['start_pipeline · EmptyOperator', 'Mốc bắt đầu, không xử lý dữ liệu.'],
    ['validate_raw_data · Sensor', 'Chờ manifest; kiểm tra ngày, partition, file/header; đóng băng context và model cho lần chạy.'],
    ['submit_pyspark_etl · SparkSubmit', 'Làm sạch, tách dữ liệu RFM và audit, ghi Parquet.'],
    ['compute_rfm_metrics · RfmSparkSubmit', 'Tính RFM, nạp model; kiểm tra và công bố RFM Asset riêng cho Marketing.'],
    ['detect_anomalies · AuditSparkSubmit', 'Đánh giá dòng/đơn; kiểm tra và công bố Anomaly Asset độc lập.'],
    ['notify_completion · PythonOperator', 'Chờ hai nhánh; đối soát marker, ghi completion và phát snapshot tổng hợp cùng curated Asset.'],
], [3900, 5700])
p('Hai nhánh sau ETL độc lập về phụ thuộc. Pool retail_spark hiện có 1 slot nên các job Spark thực tế chạy lần lượt để tiết kiệm RAM.', 'Note')

page()
p('DỮ LIỆU, PHÂN TÍCH VÀ MODEL', 'Title')
p('4. ETL và các đầu ra chính', 'Heading1')
p('CSV gốc được giữ nguyên; landing theo ngày có source version và checksum. ETL dùng schema cố định, loại dòng trùng, kiểm tra null/rỗng, kiểu dữ liệu, timestamp và giá trị không hữu hạn; lỗi parse được tách để truy vết.')
table(['Đầu ra', 'Nội dung / lý do'], [
    ['Giao dịch sạch', 'Phục vụ RFM: loại dữ liệu thiếu bắt buộc, đơn hủy, Quantity/UnitPrice không dương và phí dịch vụ; Parquet phân vùng year/month.'],
    ['Dữ liệu audit', 'Giữ dữ liệu ít bị lọc hơn, gồm cả thiếu CustomerID, hoàn/hủy và giá trị bất thường để đánh giá đúng ngữ cảnh.'],
    ['Phân tích', 'RFM theo khách, hồ sơ cụm; đánh giá từng dòng/từng đơn và danh sách đơn bị gắn cờ.'],
    ['Manifest / summary', 'Ghi cutoff, nguồn, phiên bản code/model, hash, số bản ghi, đường dẫn đầu ra và kết quả kiểm tra.'],
], [2400, 7200])
p('5. RFM, phân cụm và nhãn nghiệp vụ', 'Heading1')
p('R (Recency) = số ngày từ lần mua gần nhất đến cutoff; F (Frequency) = số hóa đơn khác nhau; M (Monetary) = tổng Quantity × UnitPrice, đơn vị GBP. R càng thấp càng tốt, F/M càng cao càng tốt. Điểm 1–5 dùng percent_rank trên Window của Spark, giữ nguyên quan hệ đồng hạng.')
p('KMeans dùng các đặc trưng log(1 + R/F/M) và StandardScaler; không dùng trực tiếp điểm R/F/M làm đầu vào. Model của lần chạy đã kiểm chứng có k=2. Cluster là mã cụm, cần đọc hồ sơ cụm để diễn giải; không tự coi Cluster 0 là VIP.')
p('Nhãn nghiệp vụ được tính riêng: high-value khi M_score ≥ 4; inactive khi Recency ≥ 90 ngày (mặc định). Đây là chỉ báo không hoạt động, không phải xác suất dự báo khách rời bỏ. Thiếu thời gian quan sát thì ghi trạng thái chưa đủ lịch sử.')
p('6. Vì sao tách training khỏi daily?', 'Heading1')
p('Training: validate_training_input → train_rfm_model → publish_model. DAG đọc snapshot ETL đã hoàn tất, fit và lưu toàn bộ scaler/KMeans PipelineModel; chỉ kích hoạt sau khi kiểm tra đạt. Training thất bại không thay model đang phục vụ.')
p('Daily chỉ load/transform bằng model được ghim trong context; không fit lại mỗi ngày. Điều này giữ scaler và tâm cụm nhất quán giữa các ngày dùng cùng model. Tuy vậy, điểm percent_rank và thành viên nhóm vẫn có thể thay đổi theo dữ liệu.')
p('Khởi tạo lần đầu: landing → bootstrap ETL-only → training model v1 → daily. Training cutoff phải nhỏ hơn scoring cutoff, model phải có hiệu lực và tương thích nguồn/đặc trưng. Bootstrap phá vòng phụ thuộc “chưa có model thì daily không chạy được”.')
p('7. Phát hiện bất thường theo phiên bản hiện tại', 'Heading1')
p('Detector có 14 kiểm tra: 7 luật chất lượng dữ liệu và 7 luật nghiệp vụ. Phân loại dòng/kênh bán trước khi đánh giá; dùng baseline theo khách hàng hoặc kênh, thống kê robust theo median/MAD (ngưỡng modified z-score mặc định 3,5) kết hợp ngưỡng mức độ đáng kể. Có kiểm tra tổng đơn, không chỉ từng dòng.')
p('Kết quả nêu flagged / not_flagged / not_applied, lý do, bằng chứng, severity và value_at_risk. Thiếu CustomerID không tự động là bất thường. Cờ là đối tượng cần xem xét, không phải kết luận gian lận; một số luật còn cần hiệu chỉnh để giảm cờ mức thấp.', 'Note')

page()
p('VẬN HÀNH, ĐỘ TIN CẬY VÀ TRUY VẾT', 'Title')
p('8. Thời gian và lịch chạy', 'Heading1')
p('Daily dùng cron 0 0 * * * theo UTC; training dùng 0 0 1 * *. Lịch được giới hạn trong giai đoạn dữ liệu lịch sử; catchup=False và DAG mặc định paused. Không lấy ngày máy hiện tại để tính RFM của bộ dữ liệu 2011.')
p('Ví dụ manual: business_date=2011-12-09 → cutoff/run_date=2011-12-10 00:00 UTC; chỉ xét giao dịch trước cutoff. Snapshot daily chứa lịch sử tích lũy đến cutoff, không chỉ giao dịch của riêng ngày 09/12. Chạy training thủ công cần training_cutoff.')
p('Catchup là scheduler tự tạo các kỳ còn thiếu; backfill là chủ động xử lý một dải ngày quá khứ. depends_on_past=False: không buộc task ngày trước phải thành công, nhưng nguồn và model của ngày chạy vẫn phải hợp lệ.')
p('9. Xử lý lỗi, chạy lại và công bố an toàn', 'Heading1')
table(['Cơ chế', 'Cấu hình / ý nghĩa'], [
    ['Chờ dữ liệu', 'Sensor reschedule mỗi 60 giây, tối đa 30 phút; trả worker slot khi chờ. Sensor không retry và không soft-fail.'],
    ['Retry / timeout', 'Daily mặc định retry 1 lần sau 5 phút, backoff tối đa 15 phút. Spark ETL/RFM timeout 20 phút; audit 30 phút.'],
    ['Tài nguyên / deadline', 'max_active_runs=1; daily max_active_tasks=2; Spark pool 1 slot, driver 3 GB. Cảnh báo sau 60 phút từ lúc DagRun queued; timeout DagRun 90 phút.'],
    ['Điều kiện thành công', 'notify_completion dùng all_success. Một nhánh lỗi chặn snapshot tổng hợp, nhưng không chặn Asset của nhánh đã thành công. Callback ghi log cục bộ.'],
    ['Chạy lại an toàn', 'Ghim context/source/model; ghi output theo phiên bản/attempt, đối chiếu hash và manifest trước khi thay con trỏ công bố bằng atomic replace có khóa.'],
], [2450, 7150])
p('Mỗi nhánh ghi marker riêng trước khi task success; lỗi gate/ghi file chặn Asset nhánh đó. Retry dùng lại snapshot đã công bố; muốn đổi generation phải tạo DagRun mới. Notify đối soát cả hai marker trước khi công bố snapshot tổng hợp. File ghi dở chưa là dữ liệu sẵn sàng.')
p('10. Asset scheduling và OpenLineage', 'Heading1')
p('RFM và Anomaly tự phát Asset khi kiểm tra đạt, không chờ nhau. Marketing chỉ nghe RFM; Dashboard chỉ nghe RETAIL_SNAPSHOT_ASSET do notify phát sau khi hai nhánh hợp lệ. Dashboard xác minh completion manifest, hash và marker của đúng run; không cần ghép hai event bằng AND. Pool 1 slot vẫn giới hạn JVM chạy đồng thời.')
p('Hai consumer hiện chỉ kiểm tra metadata và ghi log demo; chưa đọc Parquet, gửi voucher/email hay cập nhật dashboard ngoài. Asset event chứa metadata, không mang toàn bộ dataset.')
p('OpenLineage dùng để truy vết input → task → output. Extractor đọc manifest nhỏ của đúng run; ghi event ra file local, và có tùy chọn gửi đồng thời tới Marquez local để xem đồ thị. Đầu ra published chỉ được khai báo sau công bố thành công.')
p('Giới hạn: chưa có Spark listener hay lineage từng cột; transport là best effort, chưa đảm bảo phát lại event bị mất. Marquez đã kiểm chứng bằng fixture; chưa có lần chạy retail đầy đủ mới sau khi bật collector.', 'Note')
p('Thuật ngữ cần nhớ: DAG = đồ thị phụ thuộc không chu trình; TaskInstance = một task trong một lần chạy; Operator = cách thực thi; Sensor = chờ điều kiện; XCom = metadata nhỏ; Connection = cấu hình kết nối; Hook = lớp giao tiếp hệ thống ngoài. LocalExecutor của Airflow khác Spark executor.', 'Note')

page()
p('BẰNG CHỨNG VÀ CÁCH TRÌNH BÀY', 'Title')
p('11. Kết quả thực tế đã xác minh', 'Heading1')
p('Ngày 11/10/2026, scheduler thực thi thành công 4 DAG / 11 task ngay lần đầu: training (3), daily (6), Marketing (1), Executive (1). Bootstrap ETL chạy trước và không được tính vào 11 task. Hai consumer được kích hoạt bằng Asset, không phải trigger thủ công.')
table(['Chỉ số của daily ngày nghiệp vụ 09/12/2011', 'Kết quả'], [
    ['Dòng nguồn / dòng trùng loại bỏ / lỗi parse', '541.909 / 5.268 / 0'],
    ['Giao dịch sạch cho RFM / số khách hàng', '391.057 / 4.334'],
    ['Dòng được audit / dòng bị gắn cờ', '536.641 / 46.341'],
    ['Đơn được audit / đơn bị gắn cờ', '25.900 / 54'],
    ['Đơn không bị gắn cờ / không đủ điều kiện đánh giá', '19.852 / 5.994'],
    ['Thời gian daily / model', 'Khoảng 8 phút 12 giây / k=2'],
], [6550, 3050])
p('Đọc lại Parquet bằng PyArrow xác nhận số dòng khớp manifest, đủ _SUCCESS, hash nguồn/context/ETL/model hợp lệ và CSV gốc không đổi. Ba Asset events cùng publication; Marketing nhận event 2, Executive nhận 2 và 3. Đây là bằng chứng pipeline thật, vượt mức chỉ “container healthy” hay script chạy riêng.')
p('Bằng chứng ngày 11/10 thuộc thiết kế cũ phát cả ba Asset tại notify. Thiết kế hai nhánh độc lập và Dashboard dùng snapshot ngày 12/10 đã qua kiểm thử gate, retry, consumer và lineage; chưa chạy lại full dataset qua scheduler, chưa đo lợi ích độ trễ. Không dùng số liệu cũ để tuyên bố đã demo luồng mới.')
p('12. Các giới hạn cần nói thẳng', 'Heading1')
p('• Spark local dùng chung CPU/RAM với Airflow, chưa chứng minh cluster nhiều máy hay hàng triệu dòng/ngày. Hai nhánh chưa chạy Spark đồng thời với pool hiện tại.')
p('• DAG đang dùng snapshot ETL đầy đủ. CLI RFM có incremental store riêng, nhưng chưa tích hợp cơ chế đó vào DAG; Window xếp hạng toàn bộ khách vẫn là giới hạn mở rộng.')
p('• Churn chỉ là inactivity proxy; anomaly là đánh giá hồi cứu, chưa có nhãn chuẩn để kết luận độ chính xác. 536.641 là số dòng audit, không phải số bất thường.')
p('• Audit từng gặp cảnh báo codegen và spill xuống đĩa nhưng đã hoàn tất. Sau demo các DAG được pause; kết quả này không khẳng định hệ thống đang tự chạy hằng ngày ở thời điểm đọc tài liệu.')
p('13. Thứ tự nói khi thuyết trình (khoảng 8–10 phút)', 'Heading1')
p('1) Mục tiêu, dữ liệu và Airflow khác Spark (1 phút). 2) Graph 6 task, hai nhánh và pool (2 phút). 3) RFM, training riêng và audit 14 luật (2 phút). 4) Cutoff, retry, publication và Asset/lineage (2 phút). 5) Mở Grid/log/summary đối chiếu các con số trên, chốt giới hạn (1–3 phút).')
p('Câu mở đầu gợi ý: “Nhóm xây dựng hệ thống batch bán lẻ, trong đó Airflow điều phối các job PySpark. Đầu ra gồm giao dịch sạch, phân tích RFM và kiểm tra bất thường; kết quả chỉ được công bố khi các nhánh đã hoàn tất và kiểm tra đạt.”', 'Note')
p('Nguồn đối chiếu trong repository', 'Heading1')
p('Code: dags/ecommerce_etl_dag.py; retail_rfm_training_dag.py; hai consumer DAG; retail_support/tasks.py, lineage.py; scripts/retail_pipeline.py, pyspark_rfm.py, pyspark_anomalies.py. Bằng chứng: docs/RUN_EVIDENCE_20261011.md; ASSET_SCHEDULING_VI.md; OPENLINEAGE_VI.md; RFM_DAG_HANDOFF_VI.md; .agent/HANDOFF.md. DAG_DESIGN_VI.md/REPORT.md còn một số mô tả cũ; bản tóm tắt ưu tiên code và bằng chứng mới.', 'Small')

styles = f'''<w:styles xmlns:w="{W}">
<w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" w:eastAsia="Calibri"/><w:sz w:val="21"/><w:lang w:val="vi-VN"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:spacing w:after="90" w:line="250" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults>
<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/></w:style>
<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:basedOn w:val="Normal"/><w:pPr><w:keepNext/><w:spacing w:after="140"/></w:pPr><w:rPr><w:b/><w:color w:val="18334D"/><w:sz w:val="32"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:pPr><w:keepNext/><w:outlineLvl w:val="0"/><w:spacing w:before="130" w:after="70"/></w:pPr><w:rPr><w:b/><w:color w:val="007F86"/><w:sz w:val="25"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Cell"><w:basedOn w:val="Normal"/><w:pPr><w:spacing w:after="20" w:line="230" w:lineRule="auto"/></w:pPr><w:rPr><w:sz w:val="20"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Subtitle"><w:basedOn w:val="Normal"/><w:rPr><w:color w:val="526373"/><w:sz w:val="20"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Note"><w:basedOn w:val="Normal"/><w:pPr><w:shd w:fill="EDF5F6"/></w:pPr><w:rPr><w:sz w:val="20"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Flow"><w:basedOn w:val="Note"/><w:rPr><w:b/><w:color w:val="18334D"/><w:sz w:val="20"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Small"><w:basedOn w:val="Normal"/><w:rPr><w:sz w:val="17"/><w:color w:val="526373"/></w:rPr></w:style>
</w:styles>'''
document = f'<w:document xmlns:w="{W}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><w:body>' + ''.join(parts) + '''<w:sectPr><w:footerReference w:type="default" r:id="rId2"/><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="850" w:right="1050" w:bottom="850" w:left="1050" w:header="350" w:footer="400"/></w:sectPr></w:body></w:document>'''
footer = f'<w:ftr xmlns:w="{W}"><w:p><w:pPr><w:jc w:val="right"/></w:pPr><w:r><w:rPr><w:sz w:val="17"/><w:color w:val="526373"/></w:rPr><w:t>TÓM TẮT DAG  •  </w:t></w:r><w:fldSimple w:instr="PAGE"/></w:p></w:ftr>'
rels_ns = 'http://schemas.openxmlformats.org/package/2006/relationships'
office_rel = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/'
files = {
    '[Content_Types].xml': '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/>' + ''.join(f'<Override PartName="/word/{name}.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.{kind}+xml"/>' for name, kind in [('document', 'document.main'), ('styles', 'styles'), ('footer1', 'footer')]) + '</Types>',
    '_rels/.rels': f'<Relationships xmlns="{rels_ns}"><Relationship Id="rId1" Type="{office_rel}officeDocument" Target="word/document.xml"/></Relationships>',
    'word/_rels/document.xml.rels': f'<Relationships xmlns="{rels_ns}"><Relationship Id="rId1" Type="{office_rel}styles" Target="styles.xml"/><Relationship Id="rId2" Type="{office_rel}footer" Target="footer1.xml"/></Relationships>',
    'word/document.xml': document, 'word/styles.xml': styles, 'word/footer1.xml': footer,
}
with ZipFile(OUTPUT, 'w', ZIP_DEFLATED) as archive:
    for name, content in files.items():
        ET.fromstring(content)
        archive.writestr(name, '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + content)
with ZipFile(OUTPUT) as archive:
    assert archive.testzip() is None
    tree = ET.fromstring(archive.read('word/document.xml'))
    text = ' '.join(t.text or '' for t in tree.findall(f'.//{{{W}}}t'))
    assert '\ufffd' not in text
    assert len(tree.findall(f'.//{{{W}}}br')) == 3
    for fact in ['541.909', '391.057', '4.334', '536.641', '46.341', '25.900', '14 kiểm tra', 'OpenLineage', 'all_success']:
        assert fact in text, fact
print(f'Created {OUTPUT.name}; OOXML/ZIP valid; 4 designed pages; {len(text.split())} whitespace-separated words; {OUTPUT.stat().st_size} bytes.')
