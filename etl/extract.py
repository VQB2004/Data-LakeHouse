import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from prefect import task

engine = create_engine("mysql+pymysql://root:qb123@localhost:3306/raw")

def load_checkpoint():
    with open("D:\Trino\etl\checkpoint.txt","r") as f:
        return f.read()

def save_checkpoint(num):
    with open("D:\Trino\etl\checkpoint.txt","w") as f:
        f.write(str(num))

@task(cache_policy=None)
def extract():
    checkpoint = load_checkpoint()
    query = f""" SELECT * FROM giao_dich
                WHERE ngay > '{checkpoint}'
                """

    df = pd.read_sql(query, engine)

    new_checkpoint = df["ngay"].max()
    save_checkpoint(new_checkpoint)

    # ---- Lưu trực tiếp xuống MinIO ----
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df.to_parquet(
        f"s3://lakehouse/raw/giao_dich_{timestamp}.parquet",
        engine="pyarrow",
        index=False,
        storage_options={
            "key": "admin",
            "secret": "minio123",
            "client_kwargs": {
                "endpoint_url": "http://localhost:9000"
            }
        }
    )

    print("Đã extract dữ liệu vào MinIO thành công!")
