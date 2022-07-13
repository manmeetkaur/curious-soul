"""
This example demonstrates how we can create our own Custom Operators in Airflow using
BaseOperator and by extending an existing Operator as well.
"""
import os
import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from operators.transform_operator import SqlTransformOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'tags': ['etl', 'example'],
    'params': {'pipeline': 'users'}
}

"""
This is an example of extending the functionality of an existing Operator in Airflow
"""
class HttpOperator(SimpleHttpOperator):
    def __init__(self, local_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_path = local_path
    
    def execute(self, context):
        data = json.loads(super().execute(context))
        with open(self.local_path, 'w') as f:
            for row in data:
                f.write(json.dumps(row) + '\n')


@dag(default_args=default_args, start_date=datetime(2022,1,1), schedule_interval=None, catchup=False)
def users_transform():

    task_check_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_test',
        endpoint='users/'
    )

    task_read_api_data=HttpOperator(
        task_id='read_api_data',
        method='GET',
        http_conn_id='api_test',
        endpoint='users/',
        local_path=os.path.join(os.getcwd(), 'api_data.json')
    )

    task_upload_to_gcs=LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=os.path.join(os.getcwd(), 'api_data.json'),
        dst='incoming/',
        bucket='astro-fe-demo'
    )

    task_gcs_to_bigquery=GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='astro-fe-demo',
        source_objects=['incoming/api_data.json'],
        destination_project_dataset_table="astronomer-field.airflow_test_1.users",
        write_disposition='WRITE_TRUNCATE',
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=False,
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        ]
    )

    task_transform=SqlTransformOperator(
        task_id='transform',
        conn_id='google_cloud_default',
        table_name='users',
        schema_name='airflow_core'
    )

    """
    Below is the redundant code that we have included in a custom operator called SqlTransformOperator
    """
    # @task
    # def transform():
    #     print(get_current_context())
    #     params = get_current_context()['params']
    #     table_name = params['pipeline']
    #     with open(os.path.join(os.getcwd(), 'dags', 'sql', 'users.sql'), 'r') as f:
    #         raw_sql = f.read()

    #     ## Run the query and store the data in the temp table
    #     bq_hook = BigQueryHook(use_legacy_sql=False)
    #     bq_hook.run_query(
    #         sql=raw_sql, 
    #         destination_dataset_table=f'astronomer-field.airflow_test_1.temp_{table_name}',
    #         write_disposition='WRITE_TRUNCATE'
    #         )

    #     ## DQ check
    #     with open(os.path.join(os.getcwd(), 'dags', 'sql', f'dq_{table_name}.sql')) as f:
    #         raw_sql = f.read()
    #     checks_passed = bq_hook.get_pandas_df(sql=raw_sql)['check'][0]
    #     print(checks_passed)

    #     if checks_passed:
    #         bq_hook.run_query(
    #             sql=f"""create or replace table `astronomer-field.airflow_core.{table_name}` copy `astronomer-field.airflow_test_1.temp_{table_name}`
    #             """,
    #             location='US'
    #         )
    #     else:
    #         raise ValueError("DQ Checks Failed, skipping overriting Target Table")

    # task_check_api_active >> task_read_api_data >> task_upload_to_gcs >> task_gcs_to_bigquery >> transform()
    task_check_api_active >> task_read_api_data >> task_upload_to_gcs >> task_gcs_to_bigquery >> task_transform

my_dag = users_transform()
