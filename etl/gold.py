from etl.common.get_spark import create_spark_session
import pandas as pd
from prefect import task
from pyspark.sql import functions as F
import time



def get_snapshot_info(spark):
    df_snapshots = spark.sql("SELECT * FROM iceberg.silver.giao_dich.history")
    
    rows = df_snapshots.orderBy(df_snapshots.made_current_at.desc()).limit(2).collect()
    last_row, pre_last_row = rows[0], rows[1]
    return last_row, pre_last_row

@task(cache_policy=None)
def gold_etl(spark):

    last_row, pre_last_row = get_snapshot_info(spark=spark)

    if pre_last_row.parent_id is None:
         df= spark.read \
            .format("iceberg") \
            .load("iceberg.silver.giao_dich")
    else:
        df = spark.read \
            .format("iceberg") \
            .option("start-snapshot-id", pre_last_row.snapshot_id) \
            .option("end-snapshot-id", last_row.snapshot_id) \
            .load("iceberg.silver.giao_dich")



    df_fact_giao_dich = df.select(
        "ma_giao_dich",
        "ma_khach_hang",
        "ma_san_pham",
        "ngay",
        "thoi_gian",
        "phuong_thuc_giao_hang",
        "phuong_thuc_thanh_toan",
        "trang_thai_don_hang",
        "phan_hoi",
        "so_luong",
        "gia",
        "tong_tien",
        "danh_gia"
    )
    df_fact_giao_dich= df_fact_giao_dich.orderBy(F.col("ma_giao_dich").asc())
    df_fact_giao_dich.write.format("iceberg").mode("append").save("iceberg.gold.fact_giao_dich")
    

    df_kh = df.join(
        spark.table("iceberg.gold.dim_khach_hang"),
        on="ma_khach_hang",
        how="left_anti"
    )

    df_dim_khach_hang = df_kh.select(
        "ma_khach_hang",
        "ten",
        "email",
        "so_dien_thoai",
        "dia_chi",
        "thanh_pho",
        "zipcode",
        "quoc_gia",
        "tuoi",
        "gioi_tinh",
        "phan_khuc_khach_hang"
    ).dropDuplicates(["ma_khach_hang"])

    df_dim_khach_hang = df_dim_khach_hang.orderBy(F.col("ma_khach_hang").asc())
    df_dim_khach_hang.write.format("iceberg").mode("append").save("iceberg.gold.dim_khach_hang")
    

    df_sp=df.join(
        spark.table("iceberg.gold.dim_san_pham"),
        on="ma_san_pham",
        how="left_anti"
    )
    df_dim_san_pham = df_sp.select(
        "ma_san_pham",
        "loai_san_pham",
        "thuong_hieu",
        "san_pham"
    ).dropDuplicates(["ma_san_pham"])

    df_dim_san_pham = df_dim_san_pham.orderBy(F.col("ma_san_pham").asc())
    df_dim_san_pham.write.format("iceberg").mode("append").save("iceberg.gold.dim_san_pham")

    print(f"==== Đã load dữ liệu mới từ Silver vào Gold thành công ====")


