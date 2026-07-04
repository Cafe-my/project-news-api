from src.processors.processor_raw import ProcessorRaw
from src.utils.partitions import format_partition_key
from src.utils.cursor import write_cursor
from src.utils.cursor import check_connection

from datetime import datetime, UTC
import boto3
from io import BytesIO
import logging
import os
import time

#from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("NewsAPIJob") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
#     .getOrCreate() 

# ___________
# Apenas para desenvolvimento, não deve ser usado em produção
# Para poder pegar as variáveis de ambiente do arquivo .env
from dotenv import load_dotenv
load_dotenv()
# ____________


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

#provavelmente implementado em outro lugar (DAG do Airflow)
datetime_utc = datetime.now(UTC)
file_name = datetime_utc.strftime("%Y%m%dT%H%M%S")

def task_raw():
    logger.info("-------------------->> Iniciando job RAW <<--------------------") 
    
    def operator_raw(
            categories, 
            datetime_utc=datetime_utc, 
            file_name=file_name
    ):
        s3 = boto3.client("s3",region_name=os.getenv("AWS_DEFAULT_REGION"))
        check_connection(s3, [DESTINATION_BUCKET])

        failed_categories = []
        for category in categories:
            try:
                params = {
                    "apiKey": API_KEY,
                    "category": category,
                }
                final_json, total_results = ProcessorRaw(API_LINK, params, datetime_utc).execute()

                logger.info(f"Total de resultados para {category}: {total_results}")

                partition_key = format_partition_key(datetime_utc)
                with BytesIO(final_json.encode("utf-8")) as file_obj:
                    s3.upload_fileobj(file_obj, DESTINATION_BUCKET, f"{category}/{partition_key}/data_{file_name}.json")
                    
                write_cursor(s3, DESTINATION_BUCKET, f"{category}/cursor.txt", datetime_utc)
                logger.info(f"Categoria {category} processada e armazenada com sucesso no bucket.")
                time.sleep(3)

            except Exception as e:
                # Caso o processor falhe para alguma categoria, levantará o erro e os dados daquela categoria não serão armazenados. Aqui tratamos como um log e capturamos o erro, o processo continua para as outras categorias.
                logger.info(f"Erro ao processar categoria {category}: {str(e)}")
                failed_categories.append(category)
                
        return failed_categories 
        
    try:
        API_KEY = os.getenv("API_KEY")
        API_LINK = 'https://newsapi.org/v2/top-headlines?'
        list_categories = ["business", "entertainment", "general", "health", "science", "sports", "technology"]
        DESTINATION_BUCKET = 'news-api-project-raw'
        
        failed_categories = operator_raw(list_categories, datetime_utc, file_name)

        # Verifica se houve categorias que falharam no processamento. Se sim, faz um segundo processamento apenas para essas categorias. Se ainda houver falhas, loga o erro.
        if failed_categories:
            logger.info("-------------------- Iniciando segunda tentativa de extração...")
            failed_categories = operator_raw(failed_categories, datetime_utc, file_name)
            if failed_categories:
                logger.error(f"Erro ao processar as seguintes categorias após segunda tentativa: {failed_categories}")
                raise
            else:
                logger.info("Todas as categorias processadas com sucesso na segunda tentativa.")

        logger.info("-------------------->> Job RAW concluído com sucesso! <<--------------------")
    except Exception as e:
        logger.error(f"Erro ao extrair dados da API: {e}")
        raise