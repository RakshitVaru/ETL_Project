import duckdb, os, pandas as pd


# Load data into DuckDB
def load_duckdb(df: pd.DataFrame, db_path: str, table: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {table} (
            event_date DATE,
            product TEXT,
            category TEXT,
            region TEXT,
            platform TEXT,
            genre TEXT,
            units DOUBLE,
            revenue DOUBLE,
            source TEXT
        )
    ''')
    con.execute(f"DELETE FROM {table}")
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM tmp_df")
    con.close()
