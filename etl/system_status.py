import boto3
import pymysql
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from prefect import task
# Kết nối MySQL
engine = create_engine(
    "mysql+pymysql://root:qb123@localhost:3306/metadata"
)

def insert_log_status(service_name, status, latency_ms):
    df = pd.DataFrame([{
        "ten_dich_vu": service_name,
        "trang_thai": status,
        "do_tre_ms": latency_ms,
        "timestamp_at": datetime.now()
    }])

    df.to_sql(
        "system_status_log",
        con=engine,
        if_exists="append",
        index=False
    )
def check_status(service_name, url):
    try:
        time = datetime.now()
        respone = requests.get(url=url, timeout=5)
        latency_ms = (datetime.now()-time).total_seconds()*1000

        if respone.status_code == 200:
            status ="OK"
        else:
            status ="FAIL"
    except requests.exceptions.RequestException as e:
        latency_ms = -1  # nếu lỗi thì cho -1
        status = f"FAIL"
    
    insert_log_status(service_name=service_name, status=status, latency_ms=latency_ms)

@task(cache_policy=None)
def get_status():
    check_status(service_name="minio", url="http://localhost:9000/minio/health/live")
    check_status(service_name="nessie", url ="http://localhost:19120/api/v1/config")
    check_status(service_name="trino", url="http://localhost:8080/v1/info")
