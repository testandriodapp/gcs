from airflow import DAG
from datetime import timedelta,datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


CLUSTER_NAME = 'cluster-airflow-145'
PROJECT_ID = "fiery-melody-436217-c7"
REGION="us-central1"
ZONE="us-central1-a"
BUCKET_NAME = "dataproc-staging-us-central1-1042911811064-mn0wlmby"
PYSPARK_URI='gs://my-newbq-bucket1/Dataproc'
FILE_NAME1='readbgtogcstext.py'
FILE_NAME2='readbgtogcscsv.py'

default_args={
    "depends_on_past": False
}


CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    master_machine_type="n1-standard-2",
    master_disk_size=50,
    worker_machine_type="n1-standard-2",
    num_worker=2,
    worker_disk_size=100,
    storage_bucket=BUCKET_NAME
).make()

PYSPARK_JOB1 = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":f'{PYSPARK_URI}/{FILE_NAME1}'},
}
PYSPARK_JOB2 = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":f'{PYSPARK_URI}/{FILE_NAME2}'},
}
with DAG(
    'dataproc-demo',
    default_args=default_args,
    start_date=datetime(2025,1,1),
    description='A simple DAG to create airflow',
    schedule_interval='@once',
    catchup=False,
)as dag:
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )


    submit_job1=DataprocSubmitJobOperator(
        task_id='python_task1', 
        job=PYSPARK_JOB1,
        region=REGION,
        project_id=PROJECT_ID
    )
    
    submit_job2=DataprocSubmitJobOperator(
        task_id='python_task2', 
        job=PYSPARK_JOB2,
        region=REGION,
        project_id=PROJECT_ID
    )    



    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )


create_cluster >> [submit_job1,submit_job2] >> delete_cluster