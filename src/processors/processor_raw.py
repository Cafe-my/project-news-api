from datetime import datetime

import requests

class ProcessorRaw:
    def __init__(self, url, params, datetime_utc):
        self.url = url
        self.params = params
        self.datetime_utc = datetime_utc
        self.final_json = None

    def process(self):
        response = requests.get(self.url, params=self.params)
        if response.status_code == 200:
            data = response.json()
        else:
            raise Exception(f"Failed to fetch data from {self.url}. Status code: {response.status_code}")
        
        self.final_json = {
            "metadata": {
                "ingestion_timestamp": datetime.utcnow().isoformat()
            },
            "data": data
        }

        return self
    
    def execute(self):
        self.process()
        return self.final_json
