from datetime import datetime
import requests
import json

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class ProcessorRaw:
    def __init__(self, url, params, datetime_utc):
        self.url = url
        self.params = params
        self.datetime_utc = f'{datetime_utc}'
        self.final_json = None

    def _json_processor(self):
        response = requests.get(self.url, params=self.params, timeout=30)

        # loga o status code da resposta e quantidade de resultados, para monitoramento e debugging
        if response.status_code == 200:
            data = response.json() # dados no formato JSON, já como dicionário do Python
        else:
            raise Exception(f"Falha ao buscar dados de {self.url}. Status code: {response.status_code}")
        
        self.total_results = data.get("totalResults")

        # adiciona um campo de metadata com a data e hora da execução do job, e um campo de data com os dados da API
        self.final_json = {
            "metadata": {
                "ingestion_timestamp": self.datetime_utc
            },
            "data": data
        }

        # transforma o dict em JSON string
        self.final_json = json.dumps(self.final_json) 
        return self
    
    def execute(self):
        logger.info(f"Processando dados da API: {self.url} com parâmetros: {self.params}")
        self._json_processor()
        return self.final_json, self.total_results


if __name__ == "__main__":

    processor = ProcessorRaw(
        url="https://newsapi.org/v2/top-headlines?",
        params={},
        datetime_utc=datetime.utcnow().isoformat()
    )

    result = processor.execute()

    print(result)