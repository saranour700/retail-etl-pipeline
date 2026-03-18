# Retail ETL Pipeline

End-to-end data engineering project: CSV → Python/pandas → PostgreSQL → Apache Airflow

## Tech Stack
- **Python + pandas** — extraction & transformation
- **PostgreSQL** — data warehouse (`retail_dwh`)
- **Apache Airflow** — orchestration & scheduling
- **Docker** — containerized environment

## Project Structure
retail-etl-pipeline/
├── dags/retail_etl_dag.py   # Airflow DAG
├── data/                    # Drop your CSV files here
├── include/etl.py           # ETL logic
├── postgres/init/init.sql   # Creates retail_dwh DB
├── docker-compose.yml
└── README.md

## How to Run
1. Place your CSV file(s) inside `data/`
2. Run: `docker-compose up -d`
3. Open Airflow UI: http://localhost:8080 (admin / admin)
4. Enable and trigger the `retail_etl_pipeline` DAG

## Pipeline Steps
| Step | Task | Description |
|------|------|-------------|
| 1 | check_csv_files_exist | Verifies CSV files exist in data/ |
| 2 | extract_data | Reads all CSVs into a DataFrame |
| 3 | transform_data | Cleans, normalizes, adds total_price |
| 4 | load_to_postgres | Loads into retail_sales table in retail_dwh |

## Output Table
Table: `retail_sales` in database `retail_dwh`
- All original columns (auto-detected)
- `total_price` (if quantity + price columns exist)
- `loaded_at` timestamp
