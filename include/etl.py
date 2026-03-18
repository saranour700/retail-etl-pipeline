import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/retail_dwh"
DATA_DIR = "/opt/airflow/data/"


def extract(data_dir: str = DATA_DIR) -> pd.DataFrame:
    logger.info(f"Extracting CSV files from {data_dir}")
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Extracted {len(df)} rows from {len(files)} file(s)")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting transformation...")
    logger.info(f"Columns found: {list(df.columns)}")

    # Normalize column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # Drop full duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    logger.info(f"Removed {before - len(df)} duplicate rows")

    # Drop rows where ALL values are null
    df = df.dropna(how="all")

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace("nan", pd.NA)

    # Try to parse any date-like columns automatically
    for col in df.columns:
        if "date" in col or "time" in col:
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                logger.info(f"Parsed column '{col}' as datetime")
            except Exception:
                pass

    # Try to detect and calculate total if quantity + price columns exist
    qty_col = next((c for c in df.columns if "qty" in c or "quantity" in c), None)
    price_col = next((c for c in df.columns if "price" in c or "unit" in c), None)
    if qty_col and price_col:
        try:
            df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce")
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
            df["total_price"] = df[qty_col] * df[price_col]
            logger.info(f"Calculated total_price = {qty_col} * {price_col}")
        except Exception as e:
            logger.warning(f"Could not calculate total_price: {e}")

    # Add audit timestamp
    df["loaded_at"] = pd.Timestamp.now()

    logger.info(f"Transformation complete: {len(df)} rows")
    return df


def load(df: pd.DataFrame, table_name: str = "retail_sales") -> None:
    logger.info(f"Loading {len(df)} rows into table '{table_name}'...")
    engine = create_engine(DB_CONN)

    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))
        conn.commit()

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    logger.info(f"Successfully loaded data into '{table_name}'")


def run_etl(data_dir: str = DATA_DIR) -> None:
    df_raw = extract(data_dir)
    df_clean = transform(df_raw)
    load(df_clean)
    logger.info("ETL pipeline completed successfully!")
