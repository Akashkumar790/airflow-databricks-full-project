from airflow.sdk import dag, task, asset
from pendulum import datetime
import os
from aaset_13 import fetched_data

@asset(
    schedule=fetched_data,
    uri='opt/airflow/logs/data/data_processed_data.txt',
    name= "fetched_data"
)

def process_data(self):
    
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    
    with open(self.uri,'w') as f:
        f.write(f"Data fetched on {datetime.now('Asia/Kolkata')}")
        
    print(f"Data written on {self.uri}")
    
    