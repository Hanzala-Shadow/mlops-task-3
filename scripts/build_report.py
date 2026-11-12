"""Build the submission report from recorded execution evidence (python-docx)."""

import json
from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "reports" / "evidence"
REPOSITORY = "https://github.com/Hanzala-Shadow/mlops-task-3"
records = json.loads((EVIDENCE / "storage_after_restart.json").read_text())["documents"]
runs = json.loads((EVIDENCE / "airflow_runs.json").read_text())["runs"]

doc = Document()
section = doc.sections[0]
section.page_width = Inches(8.27)
section.page_height = Inches(11.69)
section.top_margin = section.bottom_margin = Inches(0.65)
section.left_margin = section.right_margin = Inches(0.65)
for name in ("Normal", "Title", "Subtitle", "Heading 1", "Heading 2", "Caption"):
    style = doc.styles[name]
    style.font.name = "Calibri"
    style.font.color.rgb = RGBColor(0, 0, 0)
    style.paragraph_format.space_after = Pt(7)
    style.paragraph_format.line_spacing = 1.08
doc.styles["Normal"].font.size = Pt(10.5)
doc.styles["Title"].font.size = Pt(25)
doc.styles["Title"].font.bold = True
doc.styles["Subtitle"].font.size = Pt(12)
doc.styles["Heading 1"].font.size = Pt(17)
doc.styles["Heading 2"].font.size = Pt(12)
doc.styles["Caption"].font.size = Pt(9)
doc.styles["Caption"].font.italic = True


def paragraph(text, style=None):
    return doc.add_paragraph(text, style)


def heading(text):
    return doc.add_heading(text, level=1)


def table(headers, rows, widths):
    t = doc.add_table(rows=1, cols=len(headers))
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    t.autofit = False
    for col, width in zip(t.columns, widths):
        col.width = Inches(width)
    for cell, text in zip(t.rows[0].cells, headers):
        cell.text = text
    repeat = OxmlElement("w:tblHeader")
    t.rows[0]._tr.get_or_add_trPr().append(repeat)
    for values in rows:
        for cell, text in zip(t.add_row().cells, values):
            cell.text = str(text)
    borders = OxmlElement("w:tblBorders")
    for side in ("top", "left", "bottom", "right", "insideH", "insideV"):
        node = OxmlElement("w:" + side)
        for key, value in (("val", "single"), ("sz", "5"), ("color", "D9D9D9")):
            node.set(qn("w:" + key), value)
        borders.append(node)
    t._tbl.tblPr.append(borders)
    for row_index, row in enumerate(t.rows):
        for cell, width in zip(row.cells, widths):
            cell.width = Inches(width)
            cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            properties = cell._tc.get_or_add_tcPr()
            shading = OxmlElement("w:shd")
            shading.set(qn("w:fill"), "173E60" if row_index == 0 else ("F2F5F8" if row_index % 2 else "FFFFFF"))
            properties.append(shading)
            margins = OxmlElement("w:tcMar")
            for side in ("top", "left", "bottom", "right"):
                margin = OxmlElement("w:" + side)
                margin.set(qn("w:w"), "95")
                margin.set(qn("w:type"), "dxa")
                margins.append(margin)
            properties.append(margins)
            for p in cell.paragraphs:
                p.paragraph_format.space_after = Pt(2)
                for run in p.runs:
                    run.font.size = Pt(9.5)
                    if row_index == 0:
                        run.font.bold = True
                        run.font.color.rgb = RGBColor(255, 255, 255)
    paragraph("").paragraph_format.space_after = Pt(0)
    return t


def image(name, caption, width=6.97):
    p = paragraph("")
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_after = Pt(3)
    p.add_run().add_picture(str(EVIDENCE / name), width=Inches(width))
    paragraph(caption, "Caption")


def link(label, url):
    p = paragraph("")
    h = OxmlElement("w:hyperlink")
    h.set(qn("r:id"), p.part.relate_to(url, "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink", is_external=True))
    r = OxmlElement("w:r")
    props = OxmlElement("w:rPr")
    color = OxmlElement("w:color")
    color.set(qn("w:val"), "173E60")
    props.append(color)
    r.append(props)
    text = OxmlElement("w:t")
    text.text = label
    r.append(text)
    h.append(r)
    p._p.append(h)


paragraph("Currency exchange ETL pipeline", "Title")
paragraph("Task 3 implementation and execution report", "Subtitle")
paragraph("Syed Muhammad Hanzala  |  12 November 2026")
paragraph("I built a daily Airflow pipeline with Astro CLI to extract Frankfurter exchange rates, transform them into the required six-field format, and load them into MongoDB. Both manual verification runs completed successfully. All 29 tests passed, and the three stored records survived a MongoDB restart without changes.")
link("Public GitHub repository  Hanzala-Shadow/mlops-task-3", REPOSITORY)

heading("Implementation and configuration")
table(["Component", "Implementation"], [
    ("Astro and Airflow", "Astro CLI 1.47.0; Astro Runtime 3.3-12; Airflow 3.3.2"),
    ("Daily extraction", "Frankfurter v2 rates; EUR base; USD, GBP, JPY targets"),
    ("Three tasks", "Extract Currency Exchange Rates; Transform Exchange Rate Data; Load Data into MongoDB"),
    ("Named XCom transfer", "raw_exchange_rates; transformed_exchange_rates; mongodb_load_summary"),
    ("Primary data store", "MongoDB database currency_etl; collection exchange_rates"),
    ("Administration", "Mongo Express Docker service; http://localhost:8082"),
    ("Networking", "Shared airflow Docker bridge; service DNS mongodb:27017"),
    ("Persistent storage", "Named mongodb_data and mongodb_config volumes; Astro metadata and logs also persist"),
    ("Image metadata", "OCI title, description, authors, and version on all three custom images"),
], [1.65, 5.32])
paragraph("The DAG runs at 18:00 UTC daily, which is 23:00 in Asia/Karachi. Catchup is disabled and only one run is active at a time. Every run requests the latest published rates and preserves the API publication date. Weekend and holiday reruns update existing records when no new rate date is available.")
paragraph("Local passwords are generated into an ignored .env file. MongoDB and Mongo Express require authentication; their published ports bind to localhost. PostgreSQL stores Airflow metadata, while MongoDB stores the extracted business data.")

doc.add_page_break()
heading("Airflow orchestration and named XComs")
paragraph("Each task explicitly pushes or pulls a named XCom. Automatic return_value XComs are disabled, and task dependencies enforce extraction before transformation and loading.")
image("dag_code.jpg", "Figure 1. Browser screenshot of the implemented DAG task definitions and explicit XCom transfers.")
table(["Producer", "Named key", "Consumer"], [
    ("Extract", "raw_exchange_rates", "Transform"),
    ("Transform", "transformed_exchange_rates", "Load"),
    ("Load", "mongodb_load_summary", "Execution verification"),
], [1.25, 3.35, 2.37])
paragraph("HTTP requests use connection and read timeouts plus transient-error retries. Airflow allows two task retries with a one-minute delay and applies a five-minute task execution timeout. Importing the DAG does not call the API or connect to MongoDB.")

doc.add_page_break()
heading("Transformation and MongoDB loading")
paragraph("The transform verifies the base currency, the complete set of requested targets, valid publication dates, and finite positive rates. It rejects duplicate targets and malformed or incomplete responses before loading. Retrieval timestamps include the UTC offset.")
image("transformation_code.jpg", "Figure 2. Browser screenshot of the six-field mapping, unique index, and MongoDB upsert operations.")
table(["Required field", "Stored value"], [
    ("base_currency", "API base, validated against EUR"),
    ("target_currency", "API quote, validated against USD, GBP, JPY"),
    ("exchange_rate", "Positive numeric API rate"),
    ("rate_date", "API publication date in YYYY-MM-DD format"),
    ("source", "Frankfurter API"),
    ("retrieved_at", "Actual retrieval time in ISO 8601 UTC format"),
], [1.65, 5.32])
paragraph("A unique index on base_currency, target_currency, and rate_date prevents duplicate records. UpdateOne with upsert=True refreshes existing documents or inserts a newly published date. MongoDB supplies its normal _id field in addition to the required six fields.")

doc.add_page_break()
heading("Live Airflow execution")
paragraph(f"The run {runs[-1]['run_id']} completed with all three tasks in Success state. The screenshots below show the live Airflow interface; display times use Asia/Karachi, while stored retrieval timestamps use UTC.")
image("airflow_success.jpg", "Figure 3. Successful extract, transform, and load tasks in the actual Airflow graph.")
image("airflow_named_xcom.jpg", "Figure 4. The transform task exposes transformed_exchange_rates in Airflow XCom storage, including the required document fields.")

doc.add_page_break()
heading("Validation results and persisted records")
paragraph("The Astro test suite passed 29 tests. Two manual DAG runs succeeded and exposed all three named XComs. The second run matched three documents and inserted zero. Restarting MongoDB preserved all three documents exactly. Mongo Express returned HTTP 401 without credentials and HTTP 200 after authentication.")
image("validation_results.jpg", "Figure 5. Browser rendering of the captured Astro test output and actual runtime checks.", width=6.6)
table(["Base", "Target", "Exchange rate", "Rate date"], [
    (r["base_currency"], r["target_currency"], r["exchange_rate"], r["rate_date"]) for r in records
], [1.0, 1.0, 2.0, 2.97])
paragraph(f"These records were retrieved at {records[0]['retrieved_at']}. The source field is Frankfurter API for every record. Raw evidence is included in reports/evidence, including task states, XCom values, volume mounts, image labels, and the before/after persistence comparison.")
paragraph("To reproduce, install Astro CLI and Docker, run python3 scripts/setup_env.py, then ./scripts/astro.sh dev start --no-proxy. Run ./scripts/astro.sh dev pytest and python3 scripts/verify_airflow.py. Use sudo for the Astro launcher if Docker requires it.")
link("Submission repository  " + REPOSITORY, REPOSITORY)

doc.core_properties.title = "Currency exchange ETL pipeline"
doc.core_properties.subject = "Task 3 Astro Airflow ETL implementation and execution evidence"
doc.core_properties.author = "Syed Muhammad Hanzala"
for root in (doc._element, doc.styles.element):
    for border in root.xpath(".//w:pBdr"):
        border.getparent().remove(border)
target = ROOT / "reports" / "Task_3_ETL_Report.docx"
doc.save(target)
print(target)
