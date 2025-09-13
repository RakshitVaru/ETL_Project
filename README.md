
# RiskETL — Real‑World (Zero‑Cost) ETL → DuckDB → Streamlit

A **config‑driven ETL/ELT** that pulls **real public datasets**, validates them with **Pandera**, loads them into a fast local **DuckDB** warehouse, and exposes **self‑serve analytics** via **Streamlit**. 
An **Apache Airflow** DAG and lightweight **CI** are included to mirror production hygiene (In Progress).



---

## ✨ Highlights

- **Real world data**: pulls the **CFPB Consumer Complaint** bulk CSV and **UCI Bank Marketing** dataset directly over HTTPS.
- **Config‑driven ETL**: add/modify data sources in YAML;
- **Data quality (DQ)**: explicit schema checks (completeness, non‑negativity, date coercion, duplicates) → **JSON + HTML report**.
- **Warehouse**: **DuckDB** (single file) for fast local analytics. Swappable loader for Snowflake/Trino/Presto later.
- **Dashboard**: **Streamlit + Plotly** KPIs, time series, breakdowns, and an embedded DQ report.
- **Orchestration (In Progress)**: **Airflow** DAG (Docker Compose)
- **CI (In Progress)**: GitHub Actions runs tests and light lint on push/PR.

---

## 📦 Datasets

1. **CFPB Consumer Complaint Database** (public bulk CSV ZIP)  
   - Used columns → `date_received`, `product`, `issue` (as `category`), `state` (as `region`)  
   - Each complaint is mapped as a single event (`units = 1`, `revenue = 0`)

2. **UCI Bank Marketing (bank-additional-full.csv)** (CSV inside ZIP)  
   - The dataset has `month` (no exact date). We synthesize `event_date` as `YYYY‑MM‑01` for time‑series.  
   - Fields mapped → `product='term_deposit'`, `category=job`, `platform=contact`, `genre=poutcome`, `units=1`, `revenue=0`


---

## 🚀 Quickstart (local)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1) Run ETL (pulls real data over HTTPS, validates, loads into DuckDB)
python -m etl.run -c config/pipeline.yaml

# 2) Launch dashboard
streamlit run dashboards/app.py
```
Open **http://localhost:8501**. Use the **Analytics** and **Data Quality** tabs.
---

## ⚙️ Configuration (`config/pipeline.yaml`)

```yaml
sources:
  - name: cfpb_complaints
    kind: csv
    path: https://files.consumerfinance.gov/ccdb/complaints.csv.zip
    source_label: cfpb
    kwargs:
      compression: zip
      low_memory: false

  - name: bank_marketing
    kind: csv
    path: https://archive.ics.uci.edu/static/public/222/bank+marketing.zip
    source_label: bank_mkt
    kwargs:
      compression: zip
      sep: ";"
      encoding: utf-8
      low_memory: false
```

## 🧱 Data model (unified)

`warehouse/risk.duckdb` → table **`sales_facts`**

| column      | type   | notes                                              |
|-------------|--------|----------------------------------------------------|
| event_date  | DATE   | from `date_received`, or synthesized by month      |
| product     | TEXT   | CFPB product name, or `'term_deposit'`             |
| category    | TEXT   | e.g., CFPB `issue`, or UCI `job`                   |
| region      | TEXT   | e.g., US state, else `'Unknown'`                   |
| platform    | TEXT   | e.g., UCI `contact` (cellular/telephone)          |
| genre       | TEXT   | freeform tag (UCI `poutcome`)                      |
| units       | DOUBLE | count of records (defaults to 1.0)                 |
| revenue     | DOUBLE | `0.0` for these public datasets                    |
| source      | TEXT   | provenance label from config                       |

---

## ✅ Data quality

- **Pandera** schema enforces: non‑null date, non‑negative `units` and `revenue`, basic typing.
- A **JSON** and **HTML** report are written to `warehouse/dq_reports/`.  
- The Streamlit **Data Quality** tab embeds the HTML report and provides a **Download** button.

---

## 📊 Dashboard (Streamlit)

- Filters: **date** range, **region**, **source**
- KPIs: row count, units, revenue
- Charts: **events over time**, **top categories**, **events by region**
- Table: sample rows after filters
- DQ tab: embedded HTML report + JSON summary

```bash
streamlit run dashboards/app.py
```


---
## 🗂️ Repository layout

```
.
├── config/
│   └── pipeline.yaml              # sources + model/warehouse settings
├── dashboards/
│   └── app.py                     # Streamlit app (analytics + DQ)
├── etl/
│   ├── connectors/io.py           # smart CSV/ZIP reader (HTTP/local, nested ZIP support)
│   ├── extract.py                 # load config, read sources
│   ├── transform.py               # dataset mappers → unified schema
│   ├── load.py                    # write to DuckDB
│   ├── quality/dq.py              # Pandera checks + JSON/HTML report
│   └── run.py                     # orchestration glue (CLI/Airflow entry point)
├── warehouse/                     # DuckDB + DQ reports (generated)
├── airflow/
│   └── dags/risk_etl_dag.py       # optional Airflow DAG
├── tests/
│   └── test_transform.py
├── requirements.txt
└── README.md
```
