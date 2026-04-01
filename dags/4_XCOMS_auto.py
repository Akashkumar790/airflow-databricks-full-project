from airflow.sdk import dag, task

@dag(
    dag_id="XCOM",
    
)
def XCOM():

    @task.python
    def first_task():
        print("extracting data from the first task")
        fetched_data={'data':[1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data:dict):
        print("transform data from the first task")
        fetched_data=data['data']
        transfrom_data=fetched_data*2
        transfrom_data_dist={'traf_data':transfrom_data}
        return transfrom_data_dist


    @task.python
    def third_task(data:dict):
        load_data=data
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    first >> second >> third


dag = XCOM()