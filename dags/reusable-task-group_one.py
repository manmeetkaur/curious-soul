import pendulum

from airflow.models.dag import DAG
from airflow.decorators import task
from include.task_groups import ProcessNumbers


with DAG(
    dag_id="reusable_task_groups_one",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    task_group = ProcessNumbers(
        task_id="process_numbers",
        dag=dag,
        number_to_process=10
    )

    @task
    def notify():
        print("The numbers have been processed")

    task_group >> notify()

