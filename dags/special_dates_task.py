from airflow.sdk import dag, task 
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates= EventsTimetable(
    event_dates=[
        datetime(2026,2,1),
        datetime(2026,2,4),
        datetime(2026,2,12),
        datetime(2026,2,17),
        datetime(2026,2,24)
    ]
)


@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=2, day=1, tz="Asia/Kolkata"),
    end_date=datetime(year=2026,month=2,day=28,tz="Asia/Kolkata"),   
    catchup=True
)
def special_event_dag():
    @task.python
    def special_event_task(**kwargs):
        executaion_date=kwargs['logical_date']
        print(f"' Running for special event {executaion_date}'")
        
    special_event=special_event_task()
    

special_event_dag()
        
        

    
    