from processors.processor_raw import ProcessorRaw
from datetime import datetime
import boto3
#from pyspark.sql import SparkSession


# Apenas para desenvolvimento, não deve ser usado em produção
# Para poder pegar as variáveis de ambiente do arquivo .env
from dotenv import load_dotenv
import os

load_dotenv()
# ____________



def task_raw():
    # spark = SparkSession.builder \
    #     .appName("NewsAPIJob") \
    #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    #     .getOrCreate() 
    s3 = boto3.client("s3")
    
    def operator():
        datetime_utc = datetime.utcnow().isoformat()
        for category in list_categories:
            params = {
                "apiKey": API_KEY,
                "category": category,
                "sources": "bbc-news"
            }

            final_json = ProcessorRaw(API_LINK, params, datetime_utc).execute()

    try:
        logger.info("Starting the job...")
        API_KEY = os.getenv("API_KEY")
        API_LINK = 'https://newsapi.org/v2/top-headlines?'
        DESTINATION_BUCKET = 'news-api-project-raw'

        list_categories = ["business", "entertainment", "general", "health", "science", "sports", "technology"]

        operator()