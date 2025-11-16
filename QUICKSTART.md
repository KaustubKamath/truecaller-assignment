# Quick Start Guide - Docker Setup

This guide will help you get the Truecaller ETL pipeline up and running in minutes.

## Prerequisites

- Docker Desktop installed and running
- At least 4GB RAM available
- 10GB disk space

## Step 1: Setup

### Windows
```bash
setup.bat
```

### Linux/Mac
```bash
chmod +x setup.sh
./setup.sh
```

This will:
- Create necessary directories
- Copy sample input data
- Set up environment variables

## Step 2: Start Services

```bash
docker-compose up -d
```

This will start:
- PostgreSQL database
- Airflow webserver (http://localhost:8081)
- Airflow scheduler
- Initialization services (connections setup)

**Note**: First startup may take 2-5 minutes as it builds the Docker image.

## Step 3: Access Airflow UI

1. Open your browser
2. Go to: http://localhost:8081
3. Login with:
   - Username: `airflow`
   - Password: `airflow`

## Step 4: Verify Setup

1. **Check DAG is loaded:**
   - Go to DAGs page
   - Look for `user_settings_etl_daily`
   - It should be in "Paused" state initially

2. **Check connections:**
   - Go to Admin → Connections
   - Verify these connections exist:
     - `spark_default`
     - `fs_default`
     - `local_file_path`
     - `spark_local`

3. **Verify input data:**
   ```bash
   # Check sample input file exists
   ls data/input/sample_input.csv
   ```

## Step 5: Run the DAG

1. **Unpause the DAG:**
   - Go to DAGs page
   - Find `user_settings_etl_daily`
   - Toggle the "Paused" button to unpause

2. **Trigger manually:**
   - Click on the DAG name
   - Click "Play" button (▶)
   - Select "Trigger DAG"

3. **Monitor execution:**
   - Click on the DAG run to see task progress
   - Green = Success, Red = Failed
   - Click on a task to see logs

## Step 6: Check Results

After the DAG completes successfully:

```bash
# Check output directory
ls -la data/output/

# On Windows PowerShell:
dir data\output\
```

Output files should be in JSON format in the output directory.

## Common Issues

### Port 8081 already in use

Edit `.env` file and change:
```
AIRFLOW_WEBSERVER_PORT=8082
```

Then restart:
```bash
docker-compose down
docker-compose up -d
```

### DAG not appearing

1. Check logs:
   ```bash
   docker-compose logs airflow-scheduler
   ```

2. Restart scheduler:
   ```bash
   docker-compose restart airflow-scheduler
   ```

### Connection errors

Verify connections are created:
```bash
docker-compose exec airflow-webserver airflow connections list
```

If missing, restart:
```bash
docker-compose restart airflow-init-connections
```

### Spark job fails

Check Spark logs:
```bash
docker-compose logs -f airflow-scheduler | grep -i spark
```

## Stop Services

```bash
docker-compose down
```

## Clean Up (Remove Everything)

```bash
# Remove containers and volumes
docker-compose down -v

# Remove built images (optional)
docker rmi truecaller-etl-airflow-webserver
```

## Next Steps

- Modify `airflow/dags/user_events.py` to customize the DAG
- Add more input files to `data/input/`
- Check `README.md` for detailed documentation

## Need Help?

Check the full README.md for:
- Detailed configuration
- Local development setup
- Testing instructions
- Troubleshooting guide

