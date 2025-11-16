# Quickstart

## 1) Setup

Windows:
```bash
setup.bat
```

Linux/Mac:
```bash
chmod +x setup.sh
./setup.sh
```

## 2) Start
```bash
docker-compose up -d
```
Airflow UI: http://localhost:8081 (user: airflow, pass: airflow)

## 3) Trigger the DAG
In Airflow UI, find `user_settings_etl_daily`:
- Unpause
- Trigger DAG
- Choose Output format (json, csv, parquet) from the dropdown

Input file used: `data/input/sample_input.csv`
Output path: `data/output/<YYYYMMDD>/`

## 4) Stop
```bash
docker-compose down
```