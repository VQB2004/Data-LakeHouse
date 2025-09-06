import boto3
import pymysql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Kết nối MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "lakehouse"
RAW_PREFIX = "raw/"

# Kết nối MySQL
engine = create_engine(
    "mysql+pymysql://root:qb123@localhost:3306/metadata"
)

def get_files_from_mysql():
    df = pd.read_sql("SELECT ten_file FROM metastore", con=engine)
    return df["ten_file"].tolist()

def insert_file_into_mysql(file_name):
    df = pd.DataFrame([{
        "ten_file": file_name,
        "thoi_gian": datetime.now()
    }])

    df.to_sql(
        "metastore", 
        con=engine, 
        if_exists="append",  
        index=False
    )
                

def get_files_from_minio():
    """Lấy danh sách file từ MinIO."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX)
    files = []
    for obj in response.get("Contents", []):
        file_key = obj["Key"]
        if not file_key.endswith("/"):  # bỏ qua thư mục
            files.append(file_key.split("/")[-1])  # lấy tên file
    return files

def check_new_files():
    meta_files = get_files_from_mysql()
    minio_files = get_files_from_minio()

    new_files = [f for f in minio_files if f not in meta_files]
    return new_files
