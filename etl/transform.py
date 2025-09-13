import pandas as pd
from typing import Dict

# Transform each dataset to a common schema:
# event_date (date), product (str), category (str), region (str), platform
def _transform_cfpb(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_date"] = pd.to_datetime(df["date_received"], errors="coerce").dt.date
    df["product"] = df.get("product")
    df["category"] = df.get("issue")
    df["region"] = df.get("state").fillna("Unknown")
    df["platform"] = None
    df["genre"] = None
    df["units"] = 1.0
    df["revenue"] = 0.0
    df["source"] = df["_source_label"]
    out = df[["event_date","product","category","region","platform","genre","units","revenue","source"]]
    out = out.dropna(subset=["event_date"])
    return out

# Transform bank marketing dataset to common schema
def _transform_bank_marketing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    month_map = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    mnum = df["month"].str.lower().map(month_map)
    df["event_date"] = pd.to_datetime({"year": 2010, "month": mnum, "day": 1}, errors="coerce").dt.date
    df["product"] = "term_deposit"
    df["category"] = df.get("job", "Unknown")
    df["region"] = "Unknown"
    df["platform"] = df.get("contact")
    df["genre"] = df.get("poutcome")
    df["units"] = 1.0
    df["revenue"] = 0.0
    df["source"] = df["_source_label"]
    out = df[["event_date","product","category","region","platform","genre","units","revenue","source"]]
    out = out.dropna(subset=["event_date"])
    return out

# Main transform function to unify multiple datasets
def transform(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    unified = []
    for name, df in dfs.items():
        cols = set(map(str.lower, df.columns))
        if "date_received" in cols and "product" in cols:
            unified.append(_transform_cfpb(df))
        elif "month" in cols and "contact" in cols:
            unified.append(_transform_bank_marketing(df))
        else:
            # basic passthrough with best-effort date detection
            tmp = df.copy()
            date_col = None
            for cand in ["date","event_date","timestamp","dt","created_at","order_date","release_date"]:
                if cand in tmp.columns.str.lower():
                    date_col = [c for c in tmp.columns if c.lower()==cand][0]
                    break
            if date_col is not None:
                tmp["event_date"] = pd.to_datetime(tmp[date_col], errors="coerce").dt.date
            else:
                tmp["event_date"] = pd.NaT
            tmp["product"] = tmp.columns[0] if tmp.shape[1] else None
            tmp["category"] = None
            tmp["region"] = None
            tmp["platform"] = None
            tmp["genre"] = None
            tmp["units"] = 1.0
            tmp["revenue"] = 0.0
            tmp["source"] = tmp.get("_source_label", "unknown")
            unified.append(tmp[["event_date","product","category","region","platform","genre","units","revenue","source"]])
    if not unified:
        return pd.DataFrame(columns=["event_date","product","category","region","platform","genre","units","revenue","source"])
    out = pd.concat(unified, ignore_index=True)
    out["region"] = out["region"].fillna("Unknown")
    out["category"] = out["category"].fillna("Unknown")
    return out
