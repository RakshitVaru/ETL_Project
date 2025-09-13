import os, json
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from datetime import datetime

# Data quality validation and reporting
def dq_validate(df: pd.DataFrame):
    schema = DataFrameSchema({
        "event_date": Column(pa.DateTime, nullable=False),
        "product": Column(pa.String, nullable=True),
        "category": Column(pa.String, nullable=True),
        "region": Column(pa.String, nullable=True),
        "platform": Column(pa.String, nullable=True),
        "genre": Column(pa.String, nullable=True),
        "units": Column(pa.Float, Check.ge(0), nullable=False),
        "revenue": Column(pa.Float, Check.ge(0), nullable=False),
        "source": Column(pa.String, nullable=False),
    }, coerce=True)
    validated = schema.validate(df, lazy=True)
    return validated


# Generate a data quality report and save as JSON and HTML
def dq_report(df: pd.DataFrame, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    min_event_date = df["event_date"].min()
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "rows": len(df),
        "null_counts": df.isna().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
        "min_event_date": str(min_event_date) if pd.notnull(min_event_date) else None,
        "max_event_date": str(df["event_date"].max()),
        "negative_revenue_rows": int((df["revenue"] < 0).sum()),
        "negative_units_rows": int((df["units"] < 0).sum()),
        "sources": df["source"].value_counts().to_dict()
    }
    path = os.path.join(out_dir, "dq_report.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    html = f"""<html><body><h2>Data Quality Report</h2><pre>{json.dumps(report, indent=2)}</pre></body></html>"""
    with open(os.path.join(out_dir, "dq_report.html"), "w") as f:
        f.write(html)
    return path
