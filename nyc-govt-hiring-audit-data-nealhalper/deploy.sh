#!/bin/bash

# NYC Hiring Audit Pipeline Deployment Script
# This script sets up and starts the entire Airflow + PostgreSQL infrastructure

set -e

echo "========================================="
echo "NYC Hiring Audit Pipeline Deployment"
echo "========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose > /dev/null 2>&1; then
    echo "Error: docker-compose is not installed or not in PATH"
    exit 1
fi

# Create necessary directories
echo "Creating directories..."
mkdir -p data/raw data/processed data/metadata
mkdir -p airflow/logs

# Set permissions for Airflow
echo "Setting up permissions..."
sudo chown -R 50000:50000 airflow/logs

# Generate Fernet key for Airflow encryption
echo "Generating Fernet key..."
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "Generated Fernet key: $FERNET_KEY"

# Update docker-compose.yml with generated Fernet key
sed -i "s/your-fernet-key-here-32-chars-long/$FERNET_KEY/g" docker-compose.yml
sed -i "s/your-fernet-key-here-32-chars-long/$FERNET_KEY/g" airflow/airflow.cfg

# Build and start services
echo "Building Docker images..."
docker-compose build

echo "Starting PostgreSQL and Redis..."
docker-compose up -d postgres redis

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# Check PostgreSQL connection
until docker-compose exec postgres pg_isready -U airflow_user -d nyc_hiring_audit; do
    echo "Waiting for PostgreSQL..."
    sleep 5
done

echo "PostgreSQL is ready!"

# Initialize Airflow database and create admin user
echo "Initializing Airflow database..."
docker-compose --profile init up airflow-init

echo "Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "========================================="
echo "Deployment completed successfully!"
echo "========================================="
echo ""
echo "Services:"
echo "- Airflow Web UI: http://localhost:8080"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "- PostgreSQL Database: localhost:5432"
echo "  Database: nyc_hiring_audit"
echo "  Username: airflow_user"
echo "  Password: airflow_password"
echo ""
echo "- Redis: localhost:6379"
echo ""
echo "To stop all services: docker-compose down"
echo "To view logs: docker-compose logs -f [service_name]"
echo ""
echo "DataGrip Connection String:"
echo "jdbc:postgresql://localhost:5432/nyc_hiring_audit"
echo ""
