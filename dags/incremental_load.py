from airflow.sdk import dag, task 
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    #dag_id='incremental_load',
    schedule=CronDataIntervalTimetable("@daily",timezone="Asia/Kolkata"),
    start_date=datetime(year=2026, month=2, day=27, tz="Asia/Kolkata"),
    end_date=datetime(year=2026,month=2,day=28,tz="Asia/Kolkata"),   
        catchup=True)
    

def incremental_load():
    @task.python
    def incremental_data_fatch(**kwargs):
        date_interval_start=kwargs["data_interval_start"]
        date_interval_end=kwargs["data_interval_start"]
        print(f"echo fetching data :{date_interval_start} to {date_interval_end}")
    @task.bash
    def incremental_data_process():
        return "echo 'processing data from {{data_interval_start}} to {{data_interval_end}}'"
    
    fetch=incremental_data_fatch()
    process=incremental_data_process()
    
    fetch >> process

dage=incremental_load()
        
        
    
    
incremental_load()
    
    
