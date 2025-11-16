#!/bin/bash
# Initialize Airflow connections

set -e

echo "Creating Airflow connections..."

# Function to add connection if it doesn't exist
add_connection_if_not_exists() {
    local conn_id=$1
    local conn_type=$2
    local conn_host=${3:-""}
    local conn_port=${4:-""}
    local conn_extra=${5:-"{}"}
    
    # Check if connection exists
    if airflow connections get "$conn_id" >/dev/null 2>&1; then
        echo "Connection '$conn_id' already exists, skipping..."
    else
        echo "Creating connection '$conn_id'..."
        if [ -n "$conn_port" ]; then
            airflow connections add "$conn_id" \
                --conn-type "$conn_type" \
                --conn-host "$conn_host" \
                --conn-port "$conn_port" \
                --conn-extra "$conn_extra"
        else
            airflow connections add "$conn_id" \
                --conn-type "$conn_type" \
                --conn-extra "$conn_extra"
        fi
    fi
}

# Create Spark connection (spark_default)
add_connection_if_not_exists 'spark_default' 'spark' 'local[*]' '7077' '{"queue": "default", "deploy-mode": "client"}'

# Create File System connection (fs_default)
add_connection_if_not_exists 'fs_default' 'fs' '' '' '{}'

# Create local_file_path connection (for FileSensor)
add_connection_if_not_exists 'local_file_path' 'fs' '' '' '{}'

# Create spark_local connection (alternative name used in DAG)
add_connection_if_not_exists 'spark_local' 'spark' 'local[*]' '7077' '{"queue": "default", "deploy-mode": "client"}'

echo "Airflow connections created successfully!"

