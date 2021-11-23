from airflow import DAG

from airflow.providers.amazon.aws.operators.emr_modify_cluster import EmrModifyClusterOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

from datetime import datetime

with DAG('emr_modify', start_date=datetime(2021,11,22), schedule_interval=None, max_active_runs=1, catchup=False) as dag:

    check_cluster_status=EmrJobFlowSensor(
        task_id='check_cluster_status',
        job_flow_id='j-ABCDEFGH',
        aws_conn_id='aws_default',
        target_states=['WAITING'],
        do_xcom_push=True
    )

    increase_step_concurrency=EmrModifyClusterOperator(
        task_id='increase_step_concurrency',
        cluster_id='j-ABCDEFGH',
        step_concurrency_level=2
    )

    check_cluster_status >> increase_step_concurrency