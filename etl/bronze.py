from etl.common.get_spark import create_spark_session
from etl.common.check_metadata import check_new_files, insert_file_into_mysql
from prefect.events import emit_event
from prefect import task
from pyspark.sql import functions as F
import time

@task(cache_policy=None)
def bronze_etl(spark):
  try:
    new_files = check_new_files()
    if not new_files:
      print("==== Không có file mới để load ==== ")
      return
  except Exception as e:
        print("==== Lỗi khi kiểm tra file mới:", e)

  for filename in new_files:
    try:
      
      df =spark.read.parquet(f"s3a://lakehouse/raw/{filename}")

      df =df.repartition(1).orderBy(F.col("ma_giao_dich").asc())

      df.write \
        .format("iceberg") \
        .mode("append") \
        .save("iceberg.bronze.giao_dich")

      insert_file_into_mysql(filename)
      print(f"==== Đã load file {filename} vào Bronze thành công ====")

      
    except Exception as e:
      print(f"==== Lỗi khi load file {filename} vào Bronze:", e)
      continue
