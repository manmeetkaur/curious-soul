from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.eks import EKSHook, NodegroupStates, ClusterStates
from airflow.providers.amazon.aws.operators.eks import EKSPodOperator, EKSCreateClusterOperator, EKSCreateNodegroupOperator, EKSDeleteNodegroupOperator, EKSDeleteClusterOperator
from airflow.providers.amazon.aws.sensors.eks import EKSNodegroupStateSensor, EKSClusterStateSensor

from datetime import datetime, timedelta

class EKSCreateNodegroupWithNodesOperator(EKSCreateNodegroupOperator):

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.create_nodegroup(
            clusterName=self.cluster_name,
            nodegroupName=self.nodegroup_name,
            subnets=self.nodegroup_subnets,
            nodeRole=self.nodegroup_role_arn,
            scalingConfig={
                'minSize': 2,
                'maxSize': 2,
                'desiredSize': 2
            },
            diskSize=10,
            instanceTypes=['t3.medium'],
            amiType='AL2_x86_64',
            updateConfig={
                'maxUnavailable': 1
            },
        )

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,11,22)
}

"""
This DAG provides an example to Create an EKS Cluster using EC2 Node Group and then delete the Node Group and Cluster at the end.
"""
with DAG('eks_cluster', default_args=default_args, schedule_interval=None, max_active_runs=1, catchup=False, tags=['example', 'eks']) as dag:

    create_cluster=EKSCreateClusterOperator(
        task_id='create_cluster',
        cluster_name='test',
        cluster_role_arn='arn:aws:iam::123456789012:role/eks_role',
        resources_vpc_config={
                            'subnetIds': ['subnet-12345678ab', 'subnet-12345678de', 'subnet-12345678fg']
                            , 'securityGroupIds' : ['sg-0123456789ab']
                            },
        compute=None ## Even though the source-code mentions compute parameter as Optional, but if you are going to setup Compute (NodeGroup or Fargate) later on
                     ## , you would need to set this to None, else the DAG won't compile
    )

    create_nodegroup=EKSCreateNodegroupWithNodesOperator(
        task_id='create_nodegroup',
        cluster_name='test',
        nodegroup_name='test',
        nodegroup_subnets=['subnet-12345678ab', 'subnet-12345678de', 'subnet-12345678fg'],
        nodegroup_role_arn='arn:aws:iam::123456789012:role/eks_role',
    )

    check_cluster_status=EKSClusterStateSensor(
        task_id='check_cluster_status',
        cluster_name='test',
        mode='reschedule',
        timeout=60 * 30,
        exponential_backoff=True
    )

    check_nodegroup_status=EKSNodegroupStateSensor(
        task_id='check_nodegroup_status',
        cluster_name='test',
        nodegroup_name='test',
        mode='reschedule',
        timeout=60 * 30,
        exponential_backoff=True
    )

    start_pod=EKSPodOperator(
        task_id="start_pod",
        cluster_name='test',
        pod_name="example_pod",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "ls"],
        labels={"example": "example"},
        get_logs=True,
        is_delete_operator_pod=True,
    )

    delete_nodegroup=EKSDeleteNodegroupOperator(
        task_id='delete_nodegroup',
        cluster_name='test',
        nodegroup_name='test'
    )

    check_nodegroup_termination=EKSNodegroupStateSensor(
        task_id='check_nodegroup_termination',
        cluster_name='test',
        nodegroup_name='test',
        mode='reschedule',
        timeout=60 * 30,
        target_state=NodegroupStates.NONEXISTENT
    )

    delete_cluster=EKSDeleteClusterOperator(
        task_id='delete_cluster',
        cluster_name='test'
    )

    check_cluster_termination=EKSClusterStateSensor(
        task_id='check_cluster_termination',
        cluster_name='test',
        mode='reschedule',
        timeout=60 * 30,
        target_state=ClusterStates.NONEXISTENT
    )

    chain(create_cluster, check_cluster_status, create_nodegroup, check_nodegroup_status, start_pod, delete_nodegroup, check_nodegroup_termination, delete_cluster, check_cluster_termination)
