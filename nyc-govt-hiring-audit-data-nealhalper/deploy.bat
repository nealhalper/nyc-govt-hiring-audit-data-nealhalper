@echo off
REM NYC Hiring Audit Pipeline Deployment Script for Windows
REM This script sets up and starts the entire Airflow + PostgreSQL infrastructure

echo =========================================
echo NYC Hiring Audit Pipeline Deployment
echo =========================================

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

REM Check if docker-compose is available
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: docker-compose is not installed or not in PATH
    pause
    exit /b 1
)

REM Create necessary directories
echo Creating directories...
mkdir data\raw 2>nul
mkdir data\processed 2>nul
mkdir data\metadata 2>nul
mkdir airflow\logs 2>nul

REM Generate a simple Fernet key (for development)
echo Setting up Airflow configuration...
powershell -Command "(1..32 | ForEach {'{0:X}' -f (Get-Random -Max 16)}) -join ''"

REM Build and start services
echo Building Docker images...
docker-compose build

echo Starting PostgreSQL and Redis...
docker-compose up -d postgres redis

REM Wait for PostgreSQL to be ready
echo Waiting for PostgreSQL to be ready...
timeout /t 10 /nobreak >nul

REM Initialize Airflow database and create admin user
echo Initializing Airflow database...
docker-compose --profile init up airflow-init

echo Starting Airflow services...
docker-compose up -d airflow-webserver airflow-scheduler

echo =========================================
echo Deployment completed successfully!
echo =========================================
echo.
echo Services:
echo - Airflow Web UI: http://localhost:8080
echo   Username: admin
echo   Password: admin
echo.
echo - PostgreSQL Database: localhost:5432
echo   Database: nyc_hiring_audit
echo   Username: airflow_user
echo   Password: airflow_password
echo.
echo - Redis: localhost:6379
echo.
echo To stop all services: docker-compose down
echo To view logs: docker-compose logs -f [service_name]
echo.
echo DataGrip Connection String:
echo jdbc:postgresql://localhost:5432/nyc_hiring_audit
echo.
pause
