# Airflow image cho DATN banking pipeline
# Base: Airflow 2.9 + Python 3.11 + ODBC Driver 17 + dbt-sqlserver
FROM apache/airflow:2.9.0-python3.11

USER root

# Cài ODBC Driver 17 cho SQL Server + tools
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg2 apt-transport-https unixodbc-dev g++ \
        libgl1-mesa-glx libglib2.0-0 \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl -sSL https://packages.microsoft.com/config/debian/12/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> /etc/bash.bashrc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Cài dbt trong venv riêng để tránh xung đột dependency với airflow 2.9.0
RUN python3 -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core>=1.7.0,<2.0.0" \
        "dbt-sqlserver>=1.7.0,<2.0.0" && \
    ln -sf /opt/dbt_venv/bin/dbt /usr/local/bin/dbt && \
    chown -R airflow:root /opt/dbt_venv

USER airflow

# Python deps cho extract + audit (dùng constraint file giữ airflow==2.9.0)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
        -r /tmp/requirements.txt \
        "apache-airflow-providers-slack<9.0.0" \
        "apache-airflow-providers-microsoft-mssql<4.0.0" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt"

# Cài PaddleOCR trong .venv_ocr (pipeline unstructured)
RUN python3 -m venv /opt/airflow/.venv_ocr && \
    /opt/airflow/.venv_ocr/bin/pip install --no-cache-dir \
        "paddlepaddle==3.2.2" \
        "paddleocr>=2.7.0" \
        "openpyxl>=3.1.0" \
        "flask>=3.0.0" \
        "requests>=2.31.0"

# Mặc định work dir
WORKDIR /opt/airflow
