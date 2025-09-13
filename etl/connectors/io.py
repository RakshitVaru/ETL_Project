import io, zipfile
from typing import Any, Dict
import pandas as pd

try:
    import requests
except Exception:
    requests = None

# Function to open a file or URL and return a BytesIO object
def _open_bytes(path: str) -> io.BytesIO:
    if path.startswith(("http://", "https://")):
        if requests is None:
            raise RuntimeError("requests is required to read remote ZIPs; pip install requests")
        r = requests.get(path, timeout=60)
        r.raise_for_status()
        return io.BytesIO(r.content)
    return io.BytesIO(open(path, "rb").read())

# Function to read CSV from ZIP, handling nested ZIPs if necessary
def _read_csv_from_zip(buf: io.BytesIO, sep=",", target_contains=None, read_kwargs: Dict[str, Any] = None) -> pd.DataFrame:
    read_kwargs = read_kwargs or {}
    with zipfile.ZipFile(buf) as z:
        names = z.namelist()

        # For handling nested ZIPs
        nested = [n for n in names if n.lower().endswith(".zip")]
        if nested:
            nested.sort(key=lambda n: ("additional" not in n.lower(), n.lower()))
            with z.open(nested[0]) as f:
                inner_bytes = io.BytesIO(f.read())
            with zipfile.ZipFile(inner_bytes) as z2:
                csvs = [n for n in z2.namelist() if n.lower().endswith(".csv")]
                if not csvs:
                    raise ValueError("No CSV found inside nested ZIP")
                if target_contains:
                    csvs.sort(key=lambda n: (target_contains.lower() not in n.lower(), n.lower()))
                with z2.open(csvs[0]) as fh:
                    return pd.read_csv(fh, sep=sep, **read_kwargs)

        # Single-level ZIP: pick the best CSV
        csvs = [n for n in names if n.lower().endswith(".csv")]
        if not csvs:
            raise ValueError(f"No CSV files found in ZIP. Members: {names}")
        if target_contains:
            csvs.sort(key=lambda n: (target_contains.lower() not in n.lower(), n.lower()))
        with z.open(csvs[0]) as fh:
            return pd.read_csv(fh, sep=sep, **read_kwargs)


# Main function to read various file types
def read_any(path: str, kind: str, **kwargs) -> pd.DataFrame:
    k = kind.lower()
    if k == "csv":
        # If reading from ZIP
        if str(kwargs.get("compression", "")).lower() == "zip":
            sep = kwargs.pop("sep", ",")
            target = kwargs.pop("zip_member_contains", None)
            kwargs.pop("compression", None)
            return _read_csv_from_zip(_open_bytes(path), sep=sep, target_contains=target, read_kwargs=kwargs)
        return pd.read_csv(path, **kwargs)
    elif k in ("parquet", "pq"):
        return pd.read_parquet(path, **kwargs)
    else:
        raise ValueError(f"Unsupported kind: {kind}")

# Example transformation function
def min_numeric_columns(df: pd.DataFrame) -> pd.Series:
    numeric_cols = df.select_dtypes(include='number').columns
    return df[numeric_cols].min()