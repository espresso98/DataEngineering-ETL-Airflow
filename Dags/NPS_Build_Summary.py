from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = """DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """.format(schema=schema, table=table)
    sql += select_sql
    cur.execute(sql)

    cur.execute("SELECT COUNT(1) FROM {schema}.temp_{table}""".format(schema=schema, table=table))
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError("{schema}.{table} didn't have any record".format(schema=schema, table=table))

    try:
        sql = "BEGIN;"
        sql += """DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};""".format(schema=schema, table=table)
        sql += "END;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id = "create_summary_table",
    start_date = datetime(2021,1,28),
    schedule_interval = '@monthly',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'choyoura',
        'table': 'nps_summary',
        'sql' : """SELECT LEFT(created_at, 7) as month,
                    ROUND(SUM(CASE 
                         WHEN score >= 9 THEN 1 
                         WHEN score <= 6 THEN -1 END)::float*100/COUNT(1), 2)
                    FROM raw_data.nps
                    GROUP BY 1
                    ORDER BY 1;"""
    },
    provide_context = True,
    dag = dag
)
