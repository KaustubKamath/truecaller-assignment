# Truecaller Assignment - User Settings ETL Pipeline

Daily ETL pipeline for transforming user events into aggregated settings maps using Spark and Airflow.

## Structure

```
truecaller-assignment/
├── airflow/
│   └── dags/user_events.py
├── src/
│   ├── jobs/user_settings_aggregator.py
│   └── transformations/settings_transformer.py
├── data/
│   ├── input/sample_input.csv
│   └── output/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── setup.sh / setup.bat
└── QUICKSTART.md
```

## Quickstart (Docker)

See `QUICKSTART.md` for minimal steps. Summary:

```bash
# 1) Setup
./setup.sh        # Linux/Mac
# or
setup.bat         # Windows

# 2) Start
docker-compose up -d

# Airflow UI
# http://localhost:8081  (user: airflow, pass: airflow)
```

In Airflow, open `user_settings_etl_daily`, unpause, and Trigger. Choose an Output format (json, csv, parquet) from the dropdown. Output files land under `data/output/<YYYYMMDD>/`.

6. **Check output:**
   ```bash
   ls -la data/output/
   ```

- Input (container): `/opt/airflow/data/input/sample_input.csv`
- Output (container): `/opt/airflow/data/output/<ds_nodash>`
- Output format: selectable in UI (`json`, `csv`, `parquet`)
