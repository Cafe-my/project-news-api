import logging
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class ProcessorTrusted:
    def __init__(self, df, datetime_utc, category):
        self.df = df
        self.datetime_utc = datetime_utc
        self.category = category

    def get_data(self):
        # Acessando a Struct e explodindo o array articles para criar uma linha para cada item do array  
        df = self.df.withColumn(
            "article",
            F.explode("data.articles")
        )

        df = df.select(
            F.col("article.source.id").alias("source_id"),
            F.col("article.source.name").alias("source_name"),
            F.col("article.author").alias("author"),
            F.col("article.title").alias("title"),
            F.col("article.description").alias("description"),
            F.col("article.url").alias("url"),
            F.col("article.publishedAt").alias("publishedAt"),
            F.col("metadata.ingestion_timestamp").alias("ingestion_timestamp")
        )
        self.df = df
        return self
        
    def include_category(self):
        self.df = self.df.withColumn("category", F.lit(self.category))
        return self

    def ingestion_time(self):
        self.df = self.df.withColumn("trusted_timestamp", F.lit(self.datetime_utc))
        return self

    def create_partition_key(self):
        self.df = self.df.withColumn('y', F.date_format(F.col('publishedAt'), 'yyyy-MM')) \
            .withColumn('d', F.date_format(F.col('publishedAt'), 'dd'))

    def get_cursor(self):
        # Calcula a maior data de publicação. A agregação retorna um DataFrame;
        # collect() executa a consulta e traz o resultado para o driver como uma lista de Row.
        # [0] acessa a primeira (e única) linha, e o segundo [0] acessa o valor da primeira coluna.
        cursor = self.df.agg(F.max("publishedAt")).collect()[0][0]
        self.cursor = cursor.date()
        return self

    def execute(self):
        self.get_data()
        self.include_category()
        self.ingestion_time()
        self.create_partition_key()
        self.get_cursor()
        return self.df, self.cursor