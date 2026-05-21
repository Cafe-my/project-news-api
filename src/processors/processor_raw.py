from datetime import datetime
import requests
import json

# import logging

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)

class ProcessorRaw:
    def __init__(self, url, params, datetime_utc):
        self.url = url
        self.params = params
        self.datetime_utc = f'{datetime_utc}'
        self.final_json = None

    def _json_processor(self):
        response = requests.get(self.url, params=self.params, timeout=30)
        if response.status_code == 200:
            data = response.json() # dados no formato JSON, já como dicionário do Python
        else:
            raise Exception(f"Falha ao buscar dados de {self.url}. Status code: {response.status_code}")
        
        self.final_json = {
            "metadata": {
                "ingestion_timestamp": self.datetime_utc
            },
            "data": data
        }
        self.final_json = json.dumps(self.final_json)

        return self
    
    def execute(self):
        self._json_processor()
        return self.final_json


if __name__ == "__main__":

    processor = ProcessorRaw(
        url="https://newsapi.org/v2/top-headlines?",
        params={},
        datetime_utc=datetime.utcnow().isoformat()
    )

    result = processor.execute()

    print(result)