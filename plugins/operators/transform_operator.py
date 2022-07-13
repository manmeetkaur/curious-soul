import os
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models.baseoperator import BaseOperator

class SqlTransformOperator(BaseOperator):

    def __init__(
            self, 
            table_name: str, 
            schema_name: str,
            conn_id: str, 
            use_legacy_sql: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.schema_name = schema_name
        self.use_legacy_sql = use_legacy_sql

    def execute(self, context):
        with open(os.path.join(os.getcwd(), 'dags', 'sql', 'users.sql'), 'r') as f:
            raw_sql = f.read()

        ## Run the query and store the data in the temp table
        bq_hook = BigQueryHook(use_legacy_sql=False)
        bq_hook.run_query(
            sql=raw_sql, 
            destination_dataset_table=f'astronomer-field.airflow_test_1.temp_{self.table_name}',
            write_disposition='WRITE_TRUNCATE'
            )

        ## DQ check
        with open(os.path.join(os.getcwd(), 'dags', 'sql', f'dq_{self.table_name}.sql')) as f:
            raw_sql = f.read()
        checks_passed = bq_hook.get_pandas_df(sql=raw_sql)['check'][0]
        print(checks_passed)

        if checks_passed:
            bq_hook.run_query(
                sql=f"""create or replace table `astronomer-field.airflow_core.{self.table_name}` copy `astronomer-field.airflow_test_1.temp_{self.table_name}`
                """,
                location='US'
            )
        else:
            raise ValueError("DQ Checks Failed, skipping overriting Target Table")
