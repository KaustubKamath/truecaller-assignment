@echo off
REM Setup script for Docker environment (Windows)

echo Setting up Docker environment for Truecaller ETL...

REM Create necessary directories
echo Creating directories...
if not exist "airflow\dags" mkdir airflow\dags
if not exist "airflow\logs" mkdir airflow\logs
if not exist "airflow\plugins" mkdir airflow\plugins
if not exist "airflow\scripts" mkdir airflow\scripts
if not exist "data\input" mkdir data\input
if not exist "data\output" mkdir data\output

REM Copy sample data if it exists
if exist "data\sample_input.csv" (
    echo Copying sample input data...
    copy data\sample_input.csv data\input\sample_input.csv
    echo Sample input data copied to data\input\
)

REM Create .gitkeep files
if not exist "data\output\.gitkeep" echo. > data\output\.gitkeep
if not exist "airflow\logs\.gitkeep" echo. > airflow\logs\.gitkeep
if not exist "airflow\plugins\.gitkeep" echo. > airflow\plugins\.gitkeep

echo.
echo Setup complete!
echo.
echo Next steps:
echo 1. Start the services: docker-compose up -d
echo 2. Access Airflow UI at: http://localhost:8081
echo 3. Login with username: airflow, password: airflow
echo 4. Trigger the DAG manually or wait for scheduled run
echo.

pause

