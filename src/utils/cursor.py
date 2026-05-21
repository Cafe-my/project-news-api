
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