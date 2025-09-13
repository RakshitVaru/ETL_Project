import argparse, os
from .extract import load_config, extract
from .transform import transform
from .load import load_duckdb
from .quality.dq import dq_validate, dq_report

# Main ETL function
def run(cfg_path: str):
    cfg = load_config(cfg_path)
    frames = extract(cfg)
    unified = transform(frames)
    try:
        validated = dq_validate(unified.copy())
    except Exception as e:
        print("DQ validation errors:", e)
        validated = unified
    db_path = cfg["warehouse"]["duckdb_path"]
    table = cfg["model"]["unified_table"]
    load_duckdb(validated, db_path, table)
    dq_dir = cfg["model"].get("dq_report_dir","warehouse/dq_reports")
    dq_report(validated, dq_dir)
    print("ETL complete. Warehouse:", db_path)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-c","--config", default="config/pipeline.yaml")
    args = ap.parse_args()
    run(args.config)
