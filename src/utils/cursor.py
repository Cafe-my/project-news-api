import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_connection(s3, bucket):
    '''Checa a conexão com um determinado bucket, caso conexão falhe, loga e levanta o erro para falhr task'''
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info("Conexão com bucket S3 validada com sucesso.")

    except:
        logger.error(f"Falha ao acessar bucket S3!")
        raise

def read_cursor():
    pass

def write_cursor(s3, CURSOR_BUCKET: str, CURSOR_KEY: str, datetime_utc):
    '''Escreve o cursor no bucket do S3. O cursor é a data máxima processada (horário) daquela camada, para a próxima camada processar apenas dados posteriores a essa data.'''
    
    cursor_data = datetime_utc.strftime("%Y-%m-%d %H:59:59")
    s3.put_object(
        Bucket=CURSOR_BUCKET,
        Key=CURSOR_KEY,
        Body=cursor_data
    )