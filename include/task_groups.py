from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task


class ProcessNumbers(TaskGroup):
    
    def __init__(
        self,
        task_id,
        dag,
        number_to_process,
        **kwargs
    ):

        super().__init__(
            dag=dag,
            group_id=task_id,
            **kwargs
        )

        self.number_to_process = number_to_process

        @task(task_group=self)
        def multiply(number_to_process: int):
            return number_to_process*100


        @task(task_group=self)
        def add_ten(multiplied_number: int):
            return multiplied_number+10

        add_ten(multiply(self.number_to_process))