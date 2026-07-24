from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_list_dates(cursor, cursor_main):
    if cursor_main >= cursor:
        return []
    
    dates = [
        (cursor_main + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, (cursor - cursor_main).days + 1)
    ]
    return dates


def check_connection(s3, BUCKETS: list):
    '''Checa a conexão com um determinado bucket, caso conexão falhe, loga e levanta o erro para falhar task'''
    try:
        for b in BUCKETS:
            s3.head_bucket(Bucket=b)
        logger.info("Conexão com bucket S3 validada com sucesso.")

    except Exception as e:
        logger.error(f"Falha ao acessar bucket S3: {e}!")
        raise


def read_cursor(s3, BUCKET: str, category: str):
    obj = s3.get_object(Bucket=BUCKET, Key=f'{category}/cursor.txt')
    texto = obj["Body"].read().decode("utf-8").strip()
    return datetime.strptime(texto, "%Y-%m-%d").date()


def write_cursor(s3, BUCKET: str, CURSOR_KEY: str, datetime_utc):
    '''Escreve o cursor no bucket do S3. O cursor é a data máxima processada (horário) daquela camada, para a próxima camada processar apenas dados posteriores a essa data.'''
    cursor_data = datetime_utc.strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=BUCKET,
        Key=CURSOR_KEY,
        Body=cursor_data
    )