# Truecaller Assignment - User Settings ETL Pipeline

Daily ETL pipeline for transforming user events into aggregated settings maps using Spark and Airflow.

## Project Overview

This project contains:
1. **Spark Job**: Transforms user events into aggregated settings maps
2. **Airflow DAG**: Orchestrates the daily ETL pipeline
3. **Unit Tests**: Test coverage for transformation logic
4. **Docker Setup**: Complete self-contained environment

## Project Structure

```
truecaller-assignment/
├── airflow/
│   ├── dags/
│   │   └── user_events.py          # Airflow DAG definition
│   ├── scripts/
│   │   └── init-connections.sh     # Connection initialization script
│   ├── logs/                       # Airflow logs
│   └── plugins/                    # Airflow plugins
├── src/
│   ├── jobs/
│   │   └── user_settings_aggregator.py  # Main Spark job
│   └── transformations/
│       └── settings_transformer.py      # Transformation logic
├── tests/                          # Unit tests
├── data/
│   ├── input/                      # Input data directory
│   │   └── sample_input.csv
│   └── output/                     # Output data directory
├── Dockerfile                      # Custom Airflow image with Spark
├── docker-compose.yml              # Docker Compose configuration
├── requirements.txt                # Python dependencies
├── setup.sh                        # Setup script (Linux/Mac)
├── setup.bat                       # Setup script (Windows)
└── README.md
```

## Quick Start with Docker (Recommended)

### Prerequisites
- Docker Desktop (or Docker + Docker Compose)
- 4GB+ RAM available
- 10GB+ disk space

### Setup

1. **Run setup script:**
   ```bash
   # Linux/Mac
   chmod +x setup.sh
   ./setup.sh
   
   # Windows
   setup.bat
   ```

2. **Start services:**
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow UI:**
   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

4. **Place input data:**
   ```bash
   # Copy your input file to the input directory
   cp your_input.csv data/input/sample_input.csv
   ```

5. **Trigger DAG:**
   - Go to Airflow UI → DAGs → `user_settings_etl_daily`
   - Click "Play" button to trigger manually
   - Or wait for scheduled run (daily at midnight)

6. **Check output:**
   ```bash
   ls -la data/output/
   ```

### Docker Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Restart a service
docker-compose restart airflow-scheduler

# Clean up (removes volumes too)
docker-compose down -v
```

## Local Development (Without Docker)

### Prerequisites
- Python 3.8+
- Spark 3.5.0 (or use provided setup)
- Airflow 2.8.0

### Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Spark job locally:**
   ```bash
   python -m src.jobs.user_settings_aggregator \
       --input-path data/sample_input.csv \
       --output-path data/output \
       --partition-column id \
       --input-format csv \
       --output-format csv
   ```

3. **Run tests:**
   ```bash
   pytest tests/ -v
   ```

## Spark Job

### Description
The Spark job reads user events with schema `(id, name, value, timestamp)` and aggregates them into a map where each `id` has a `settings` map containing the latest value for each setting name based on `timestamp`.

### Input Schema
```
id (long)
name (string)
value (string)
timestamp (long)
```

### Output Schema
```
id (long)
settings (Map<string, string>)
```

### Example

**Input:**
```
id,name,value,timestamp
1,notification,TRUE,1546333200
1,notification,FALSE,1546335647
1,background,TRUE,1546333546
2,background,notDetermined,1546333611
3,refresh,4,1546333443
3,refresh,denied,1546334200
```

**Output:**
```
id,settings
1,{"notification":"FALSE","background":"TRUE"}
2,{"background":"notDetermined"}
3,{"refresh":"denied"}
```

### Running the Job

```bash
python -m src.jobs.user_settings_aggregator \
    --input-path /path/to/input \
    --output-path /path/to/output \
    --partition-column id \
    --input-format parquet \
    --output-format parquet
```

## Airflow DAG

### Configuration
- **DAG ID**: `user_settings_etl_daily`
- **Schedule**: Daily at midnight (`0 0 * * *`)
- **Tasks**:
  1. `start`: Start task
  2. `check_input_data_available`: FileSensor to check for input data
  3. `run_spark_job`: SparkSubmitOperator to run the transformation
  4. `send_success_notification`: Success notification
  5. `end`: End task

### Connections
The following connections are automatically created:
- `spark_default`: Spark connection (local[*])
- `fs_default`: File system connection
- `local_file_path`: File system connection (alternative)
- `spark_local`: Spark connection (alternative)

### Paths (Docker)
- Input: `/opt/airflow/data/input/sample_input.csv`
- Output: `/opt/airflow/data/output/`

## Testing

### Run Unit Tests
```bash
pytest tests/ -v
```

### Test Coverage
```bash
pytest tests/ --cov=src --cov-report=html
```

## Data Directory Structure

```
data/
├── input/              # Place input files here
│   └── sample_input.csv
└── output/             # Spark writes output here
    └── (output files)
```

## Troubleshooting

### Port Already in Use
If port 8081 is already in use, change it in `.env`:
```bash
AIRFLOW_WEBSERVER_PORT=8082
```

### Connection Issues
Check if connections are created:
```bash
docker-compose exec airflow-webserver airflow connections list
```

### Spark Job Fails
Check Spark job logs:
```bash
docker-compose logs -f airflow-scheduler
```

### Permission Issues
On Linux/Mac, ensure proper permissions:
```bash
sudo chown -R 50000:0 airflow/logs airflow/dags data
```

## Requirements

See `requirements.txt` for Python dependencies.

## License

© 2025 True Software Scandinavia AB
