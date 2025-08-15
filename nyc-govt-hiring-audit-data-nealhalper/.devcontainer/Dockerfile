# Development Container for NYC Hiring Audit Pipeline with DBT
FROM mcr.microsoft.com/vscode/devcontainers/python:3.11

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/workspace

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    git \
    curl \
    wget \
    unzip \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js (for some DBT features)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs

# Create workspace directory
WORKDIR /workspace

# Copy requirements and install Python packages
COPY requirements.txt .
COPY requirements-dev.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-dev.txt

# Install DBT with PostgreSQL adapter
RUN pip install --no-cache-dir \
    dbt-core>=1.7.0,<1.8.0 \
    dbt-postgres>=1.7.0,<1.8.0

# Install additional development tools
RUN pip install --no-cache-dir \
    jupyter \
    jupyterlab \
    ipython \
    black \
    flake8 \
    pytest \
    pytest-cov \
    mypy \
    sqlfluff \
    great-expectations \
    evidently

# Set up non-root user
USER vscode

# Create directories for DBT and logs
RUN mkdir -p /home/vscode/.dbt /home/vscode/logs

# Set working directory
WORKDIR /workspace

# Keep container running
CMD ["sleep", "infinity"]
