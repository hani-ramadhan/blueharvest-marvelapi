#!/bin/bash

# Create required directories
mkdir -p /opt/airflow/data/marvel/{raw,logs,processed,analytics}/{latest,$(date +%Y%m%d)}

# Set ownership to airflow user
chown -R airflow:root /opt/airflow/data

# Set directory permissions
chmod -R 775 /opt/airflow/data

echo "Initialized data directories with correct permissions"