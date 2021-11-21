from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


dag = DAG(
    dag_id = 'MySQL_to_Redshift',
    start_date = datetime(2021,9,9),
    schedule_interval = '0 9 * * *',  
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "choyoura"
table = "nps"
s3_bucket = "cardon-test"
s3_key = schema + "-" + table

s3_folder_cleanup = S3DeleteObjectsOperator(
    task_id = 's3_folder_cleanup',
    bucket = s3_bucket,
    keys = s3_key,
    aws_conn_id = "aws_conn_id",
    dag = dag
)

mysql_to_s3_nps = MySQLToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = "SELECT * FROM prod.nps",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    mysql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    redshift_conn_id = "redshift_dev_db",
    dag = dag
)

s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps