from prefect import flow, task, get_run_logger
from etl.extract import extract
from etl.bronze import bronze_etl
from etl.silver import silver_etl
from etl.gold import gold_etl
from etl.send_email import send_email_notification
from etl.common.get_spark import create_spark_session
from etl.system_status import get_status



@flow
def lakehouse_etl(spark):
    try:
        extract_ok = extract()
        bronze_ok = bronze_etl(spark, wait_for=extract_ok)
        silver_ok = silver_etl(spark, wait_for=bronze_ok)
        gold_ok = gold_etl(spark, wait_for=silver_ok)
        status = get_status(wait_for=gold_ok)

        send_email_notification(success=True)
        return status

    except Exception as e:
        send_email_notification(success=False, error=str(e))
        raise 
if __name__=="__main__":
    spark = create_spark_session(app_name="ETL")
    try:
        lakehouse_etl(spark=spark)
    finally:
        spark.stop()