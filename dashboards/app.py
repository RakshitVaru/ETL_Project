import os, json, pandas as pd, duckdb, streamlit as st, plotly.express as px, yaml

st.set_page_config(page_title="RiskETL — Real‑World Analytics", layout="wide")

@st.cache_data(show_spinner=False)
def load_cfg(path="config/pipeline.yaml"):
    with open(path,"r") as f:
        return yaml.safe_load(f)

cfg = load_cfg()
DB = cfg["warehouse"]["duckdb_path"]
TABLE = cfg["model"]["unified_table"]
DQ_DIR = cfg["model"].get("dq_report_dir","warehouse/dq_reports")

def q(sql):
    con = duckdb.connect(DB, read_only=True)
    df = con.execute(sql).fetchdf()
    con.close()
    return df

st.title("RiskETL — Real‑World Analytics")
st.caption("CFPB complaints + UCI bank marketing → DuckDB, with DQ checks.")

tab1, tab2 = st.tabs(["Analytics", "Data Quality"])

with tab1:
    filt = q(f"SELECT MIN(event_date) AS min_d, MAX(event_date) AS max_d FROM {TABLE}")
    min_d = pd.to_datetime(filt['min_d'][0]).date() if filt['min_d'][0] is not None else pd.to_datetime('2000-01-01').date()
    max_d = pd.to_datetime(filt['max_d'][0]).date() if filt['max_d'][0] is not None else pd.to_datetime('2000-01-02').date()
    start, end = st.slider("Date range", min_value=min_d, max_value=max_d, value=(min_d,max_d), format="YYYY-MM-DD")

    regions = q(f"SELECT DISTINCT region FROM {TABLE} ORDER BY 1")['region'].tolist()
    r_sel = st.multiselect("Region", regions, default=regions)
    sources = q(f"SELECT DISTINCT source FROM {TABLE} ORDER BY 1")['source'].tolist()
    s_sel = st.multiselect("Source", sources, default=sources)

    start_str, end_str = start.isoformat(), end.isoformat()
    where = f"event_date BETWEEN DATE '{start_str}' AND DATE '{end_str}'"

    # Handle empty selections
    if r_sel:
        where += f" AND region IN ({', '.join([repr(r) for r in r_sel])})"
    else:
        where += " AND 1=0"  # No region selected, return no rows

    if s_sel:
        where += f" AND source IN ({', '.join([repr(s) for s in s_sel])})"
    else:
        where += " AND 1=0"  # No source selected, return no rows

    kpi = q(f"""SELECT COUNT(*) AS "rows", SUM(units) AS units, SUM(revenue) AS revenue
              FROM {TABLE} WHERE {where}""")
    c1,c2,c3 = st.columns(3)
    c1.metric("Rows", f"{int(0 if pd.isna(kpi.rows[0]) else kpi.rows[0]):,}")
    c2.metric("Units", f"{int(0 if pd.isna(kpi.units[0]) else kpi.units[0]):,}")
    c3.metric("Revenue", f"${(0 if pd.isna(kpi.revenue[0]) else kpi.revenue[0]):,.2f}")

    ts = q(f"""SELECT event_date, SUM(revenue) revenue, SUM(units) units
           FROM {TABLE} WHERE {where} GROUP BY 1 ORDER BY 1""")
    st.plotly_chart(px.line(ts, x="event_date", y="units", title="Events Over Time"), use_container_width=True)

    by_cat = q(f"""SELECT category, COUNT(*) AS "rows"
           FROM {TABLE} WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 20""")
    by_region = q(f"""SELECT region, COUNT(*) AS "rows"
           FROM {TABLE} WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 20""")
    c4,c5 = st.columns(2)
    with c4:
        st.plotly_chart(px.bar(by_cat, x="category", y="rows", title="Top Categories"), use_container_width=True)
    with c5:
        st.plotly_chart(px.bar(by_region, x="region", y="rows", title="Events by Region"), use_container_width=True)

    st.write("### Sample Rows")
    sample = q(f"SELECT * FROM {TABLE} WHERE {where} ORDER BY event_date DESC LIMIT 200")
    st.dataframe(sample, use_container_width=True)

with tab2:
    st.subheader("Data Quality Report")
    json_path = os.path.join(DQ_DIR, "dq_report.json")
    html_path = os.path.join(DQ_DIR, "dq_report.html")
    if os.path.exists(json_path):
        with open(json_path,"r") as f:
            rep = json.load(f)
        st.json(rep, expanded=False)
        if os.path.exists(html_path):
            with open(html_path, "rb") as f:
                st.download_button("Download DQ report (HTML)", data=f, file_name="dq_report.html", mime="text/html")
    else:
        st.info("Run the ETL first to generate a data quality report (see README).")
