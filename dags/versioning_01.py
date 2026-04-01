from airflow.sdk import dag, task

@dag(
    dag_id="version_task",
    
)
def version_task():

    @task.python
    def first_task():
        print("first task completed ")

    @task.python
    def second_task():
        print("second task completed ")

    @task.python
    def third_task():
        print("third welocme to apache airflow ")
    @task.python
    def versioning():
        print('this is new version')

    first = first_task()
    second = second_task()
    third = third_task()
    version01=versioning()

    first >> second  >> version01 >> third


dag = version_task()