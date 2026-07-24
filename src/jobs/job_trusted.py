
from datetime import datetime, UTC
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.cursor import check_connection, get_list_dates, read_cursor, write_cursor
from src.utils.partitions import partitions_filter, upload_data
from src.processors.processor_trusted import ProcessorTrusted

from dotenv import load_dotenv
load_dotenv()
# from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("NewsAPIJob") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

datetime_utc = datetime.now(UTC)
file_name = datetime_utc.strftime("%Y%m%dT%H%M%S")

def task_trusted():
    logger.info("-------------------->> Iniciando job TRUSTED <<--------------------") 
    
    def operator_trusted(
        categories
    ):
        s3 = boto3.client("s3",region_name=os.getenv("AWS_DEFAULT_REGION"))
        check_connection(s3, [SOURCE_BUCKET, DESTINATION_BUCKET])

        failed_categories = []
        for category in categories:
            try:
                logger.info(f"Processando categoria: {category}")

                cursor_raw = read_cursor(s3, SOURCE_BUCKET, category)
                cursor_trusted = read_cursor(s3, DESTINATION_BUCKET, category)
                logger.info(f"Cursor RAW: {cursor_raw}, Cursor TRUSTED: {cursor_trusted}")

                dates = get_list_dates(cursor_raw, cursor_trusted)
                logger.info(f"Datas a serem processadas: {dates}")

                df = partitions_filter(spark, dates, SOURCE_BUCKET, category)

                if df is not None:
                    df, cursor = ProcessorTrusted(df, datetime_utc, category).execute()
                    logger.info(f"Processamento da categoria concluído.")

                    upload_data(df, DESTINATION_BUCKET)
                    logger.info(f"Upload dos dados da categoria concluído com sucesso.")

                    write_cursor(s3, DESTINATION_BUCKET, f"{category}/cursor.txt", cursor)
                    logger.info(f"Cursor atualizado para {cursor}")
                else:
                    logger.info(f"Nenhuma nova data para processar na categoria {category}.")

            except Exception:
                logger.exception(f"Erro ao processar categoria {category}")
                failed_categories.append(category)

    try: 
        SOURCE_BUCKET = 'news-api-project-raw'
        DESTINATION_BUCKET = 'news-api-project-trusted'
        list_categories = ["business", "entertainment", "general", "health", "science", "sports", "technology"]

        operator_trusted(list_categories)
        logger.info("-------------------->> Job TRUSTED concluído com sucesso! <<--------------------")
    except Exception:
        logger.exception(f"Erro ao processar job TRUSTED")
        raise

if __name__ == "__main__":
    task_trusted()