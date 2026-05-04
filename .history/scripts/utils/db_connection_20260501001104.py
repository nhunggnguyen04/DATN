import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def build_engine(prefix: str):
    server = os.getenv(f"{prefix}_SERVER")
    port = os.getenv(f"{prefix}_PORT", "1433")
    database = os.getenv(f"{prefix}_DATABASE")
    username = os.getenv(f"{prefix}_USERNAME")
    password = os.getenv(f"{prefix}_PASSWORD")
    driver = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
        fast_executemany=True,
    )


def get_source_engine():
    return build_engine("SOURCE")


def get_target_engine():
    return build_engine("TARGET")


def test_connections():
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    source_db = pd.read_sql("SELECT DB_NAME() AS source_database", source_engine)
    target_db = pd.read_sql("SELECT DB_NAME() AS target_database", target_engine)

    print("Source connection OK")
    print(source_db)

    print("Target connection OK")
    print(target_db)


if __name__ == "__main__":
    test_connections()