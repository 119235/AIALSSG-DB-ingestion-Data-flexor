from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
import sys
import time
from airflow.models.dagrun import DagRun
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash_operator import BashOperator
import os
from airflow.operators.python import task, get_current_context
from typing import Dict, Any, List
from airflow.decorators import dag, task
import boto3
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import json
from dfmwaa import call_job_executor
import random

default_args = {
    'owner': 'MDH DevOps'
}

datasetNamePrefix = "scripts/dataset_master/Processed/cdm/dim/"
datasetName = ["mdh_bndry_centris_dim_evnt", "mdh_bndry_centris_dim_evnt_spnd", "dim_mdh_evnt", "dim_mdh_evnt_spnd"]
processName = "comm_mdh_dim_evnt"
workSpaceName = "us-dna-mdh-ops"
'''
Parameters of call_job_executor
Mandatory Parameters:
PROCESS_NAME: 
GLUE_JOB_NAME: Not Required for start and end tasks
GLUE_JOB_PARAMS: Parameters to be used while calling glue jobs. Not required for Start and end tasks 
WORKSPACE_NAME: Mandatory for worspace based application

Optional Parameters:
APPLICATION_NAME: Name of the application
SUB_APPLICATION_NAME: Name of the child application (Eg. Application ABC is a Child Application of MDH)
WORKER_TYPE: Glue Worker Type
NUMBER_OF_WORKERS: Number of Glue DPU needed
DAG_ENV: Name of the environment (dev/tst/prd). By default it is taken from MWAA name
NOTIFY: Trigger Start/End/Failure Email Notification (True/False)
'''
#[START DAG]
with DAG(
    dag_id=processName,
    default_args=default_args,
    description='',
    start_date=datetime(2023,1,31),
    schedule_interval=None,
    tags=["MDH", "CDM", "DIM", "EVNT"]
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=call_job_executor,
        op_kwargs = {'PROCESS_NAME': processName, "WORKSPACE_NAME": workSpaceName},
        provide_context=True,
        dag=dag
    )
    end = PythonOperator(
        task_id='end',
        python_callable=call_job_executor,
        op_kwargs = {'PROCESS_NAME': processName, "WORKSPACE_NAME": workSpaceName},
        provide_context=True,
        dag=dag
    )

    bndry_app_centris_evnt = PythonOperator(
        task_id='bndry_app_centris_evnt',
        python_callable=call_job_executor,
        op_kwargs = {
            'PROCESS_NAME': processName, 'GLUE_JOB_NAME': "us-dna-mdh-ops__"+processName,"GLUE_JOB_PARAMS": {"dataset_master_key": datasetNamePrefix+"bndry_app_dim_evnt.json", "enable-auto-scaling": "true", 'enable-continuous-cloudwatch-log': 'true', 'enable-continuous-log-filter': 'true', 'dataset_name': 'mdh_bndry_centris_dim_evnt'}, "WORKSPACE_NAME": workSpaceName, "APPLICATION_NAME": "", "SUB_APPLICATION_NAME": ""},
        provide_context=True,
        dag=dag
    )
    bndry_app_centris_evnt_spnd = PythonOperator(
        task_id='bndry_app_centris_evnt_spnd',
        python_callable=call_job_executor,
        op_kwargs = {
            'PROCESS_NAME': processName, 'GLUE_JOB_NAME': "us-dna-mdh-ops__"+processName,"GLUE_JOB_PARAMS": {"dataset_master_key": datasetNamePrefix+"bndry_app_dim_evnt_spnd.json", "enable-auto-scaling": "true", 'enable-continuous-cloudwatch-log': 'true', 'enable-continuous-log-filter': 'true', 'dataset_name': 'mdh_bndry_centris_dim_evnt_spnd'}, "WORKSPACE_NAME": workSpaceName, "APPLICATION_NAME": "", "SUB_APPLICATION_NAME": ""},
        provide_context=True,
        dag=dag
    )
    dim_mdh_evnt = PythonOperator(
        task_id='dim_mdh_evnt',
        python_callable=call_job_executor,
        op_kwargs = {
            'PROCESS_NAME': processName, 'GLUE_JOB_NAME': "us-dna-mdh-ops__"+processName,"GLUE_JOB_PARAMS": {"dataset_master_key": datasetNamePrefix+"dim_mdh_evnt.json", "enable-auto-scaling": "true", 'enable-continuous-cloudwatch-log': 'true', 'enable-continuous-log-filter': 'true'}, "WORKSPACE_NAME": workSpaceName, "APPLICATION_NAME": "", "SUB_APPLICATION_NAME": ""},
        provide_context=True,
        dag=dag
    )
    dim_mdh_evnt_spnd = PythonOperator(
        task_id='dim_mdh_evnt_spnd',
        python_callable=call_job_executor,
        op_kwargs = {
            'PROCESS_NAME': processName, 'GLUE_JOB_NAME': "us-dna-mdh-ops__"+processName,"GLUE_JOB_PARAMS": {"dataset_master_key": datasetNamePrefix+"dim_mdh_evnt_spnd.json", "enable-auto-scaling": "true", 'enable-continuous-cloudwatch-log': 'true', 'enable-continuous-log-filter': 'true', "APPLICATION_NAME": "", "SUB_APPLICATION_NAME": ""}, "WORKSPACE_NAME": workSpaceName},
        provide_context=True,
        dag=dag
    )

    bndry_app_centris_evnt.set_upstream(start)
    bndry_app_centris_evnt_spnd.set_upstream(start)
    dim_mdh_evnt.set_upstream(bndry_app_centris_evnt)
    dim_mdh_evnt_spnd.set_upstream(bndry_app_centris_evnt_spnd)
    end.set_upstream(dim_mdh_evnt)
    end.set_upstream(dim_mdh_evnt_spnd)