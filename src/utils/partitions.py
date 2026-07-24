from datetime import datetime

def format_partition_key(partition_key: datetime) -> str:
    if isinstance(partition_key, str):
        partition_key = datetime.strptime(partition_key, "%Y-%m-%d")

    y = partition_key.strftime("%Y-%m")
    d = partition_key.strftime("%d")
    
    return f"y={y}/d={d}"

def partitions_filter(spark, dates, BUCKET, category):
    if not dates:
        return None
    else:
        paths = [
            f"s3a://{BUCKET}/{category}/{format_partition_key(data)}"
            for data in dates
        ]

        return spark.read.json(paths)
    
def upload_data(df, BUCKET):
    '''
    Faz o upload do DataFrame para o bucket S3, particionado por categoria, ano-mes e dia
    Em caso de reprocessamento, sobrescreve os dados existentes para a mesma partição
    '''
    df.write \
    .mode("overwrite") \
    .partitionBy("category", "y", "d") \
    .parquet(f"s3a://{BUCKET}/")
