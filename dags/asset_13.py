from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

@asset(
    schedule='@daily',
    uri='opt/airflow/logs/data/data_extract.txt',
    name= "fetched_data"
)

def fetched_data(self):
    
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    
    with open(self.uri,'w') as f:
        f.write(f"Data fetched on {datetime.now('Asia/Kolkata')}")
        
    print(f"Data written on {self.uri}")
    
    