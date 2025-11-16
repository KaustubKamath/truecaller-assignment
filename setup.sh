#!/bin/bash
# Setup script for Docker environment

set -e

echo "Setting up Docker environment for Truecaller ETL..."

# Create necessary directories
echo "Creating directories..."
mkdir -p airflow/dags airflow/logs airflow/plugins
mkdir -p data/input data/output
mkdir -p airflow/scripts

# Copy sample data if it exists
if [ -f "data/sample_input.csv" ]; then
    echo "Copying sample input data..."
    cp data/sample_input.csv data/input/
    echo "Sample input data copied to data/input/"
fi

# Create .gitkeep files
touch data/output/.gitkeep
touch airflow/logs/.gitkeep
touch airflow/plugins/.gitkeep

# Set Airflow UID (Linux/Mac)
if [ "$(uname)" != "MINGW"* ] && [ "$(uname)" != "MSYS"* ]; then
    if [ -z "$AIRFLOW_UID" ]; then
        export AIRFLOW_UID=$(id -u)
        echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
    fi
fi

# Make scripts executable
chmod +x airflow/scripts/init-connections.sh 2>/dev/null || true

echo ""
echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start the services: docker-compose up -d"
echo "2. Access Airflow UI at: http://localhost:8081"
echo "3. Login with username: airflow, password: airflow"
echo "4. Trigger the DAG manually or wait for scheduled run"
echo ""

