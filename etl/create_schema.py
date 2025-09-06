from common.trino_connection import trino_query
from prefect import task
BRONZE ="""
    CREATE SCHEMA IF NOT EXISTS iceberg.bronze
"""
SLIVER ="""
    CREATE SCHEMA IF NOT EXISTS iceberg.silver
"""
GOLD ="""
    CREATE SCHEMA IF NOT EXISTS iceberg.gold
"""

def create_schema():
    trino_query(BRONZE)
    trino_query(SLIVER)
    trino_query(GOLD)
    print("==== Tạo các schema thành công ====")

create_schema()