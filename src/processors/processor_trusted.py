import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class ProcessorTrusted:
    def __init__(self, df, datetime_utc):
        self.df = df
        self.datetime_utc = datetime_utc

    def add_ingestion_timestamp(self):
        self.df.show()

    def execute(self):
        self.add_ingestion_timestamp()