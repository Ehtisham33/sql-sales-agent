import os
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f"mssql+pyodbc://{server}/{database}"
        f"?driver={driver.replace(' ', '+')}"
        f"&trusted_connection=yes"
        f"&TrustServerCertificate=yes"
    )

    return create_engine(connection_string)

def run_sql_query(sql):
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df

def extract_schema():
    engine = get_engine()
    query = """
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

    schema = {}
    for table, column in rows:
        if table not in schema:
            schema[table] = []
        schema[table].append(column)

    schema_text = ""
    for table, columns in schema.items():
        column_str = ", ".join(columns)
        schema_text += f"TABLE {table} ({column_str})\n"

    return schema_text.strip()
