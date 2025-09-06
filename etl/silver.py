from etl.common.get_spark import create_spark_session
import pandas as pd
from pyspark.sql import functions as F
from prefect import task

def get_snapshot_info(spark):
    df_snapshots = spark.sql("SELECT * FROM iceberg.bronze.giao_dich.history")
    
    rows = df_snapshots.orderBy(df_snapshots.made_current_at.desc()).limit(2).collect()
    last_row, pre_last_row = rows[0], rows[1]
    return last_row, pre_last_row

@task(cache_policy=None)
def silver_etl(spark):
   

    last_row, pre_last_row = get_snapshot_info(spark=spark)


    if pre_last_row.parent_id is None:
         df= spark.read \
            .format("iceberg") \
            .load("iceberg.bronze.giao_dich")
    else:
        df = spark.read \
            .format("iceberg") \
            .option("start-snapshot-id",pre_last_row.snapshot_id) \
            .option("end-snapshot-id",last_row.snapshot_id) \
            .load("iceberg.bronze.giao_dich")
        
    df = df.dropna() \
    .filter((df['tong_tien'] > 0)) \
    .dropDuplicates()

    df=df.orderBy(F.col("ma_giao_dich").asc())

    df.write \
        .format("iceberg") \
        .mode("append") \
        .save("iceberg.silver.giao_dich")
    print(f"==== Đã load dữ liệu mới từ Bronze vào Silver thành công ====")

