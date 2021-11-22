from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def etl(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    api_key = Variable.get("open_weather_api_key")

    # https://openweathermap.org/api/one-call-api
    url = "https://api.openweathermap.org/data/2.5/onecall"

    params = {
        "lat" : lat,
        "lon" : lon,
        "appid" : api_key,
        "units" : "metric"
    }

    response = requests.get(url, params=params)
    data = json.loads(response.text)

    """
    {'dt': 1622948400, 'sunrise': 1622923873, 'sunset': 1622976631, 'moonrise': 1622915520, 'moonset': 1622962620, 'moon_phase': 0.87, 'temp': {'day': 26.59, 'min': 15.67, 'max': 28.11, 'night': 22.68, 'eve': 26.29, 'morn': 15.67}, 'feels_like': {'day': 26.59, 'night': 22.2, 'eve': 26.29, 'morn': 15.36}, 'pressure': 1003, 'humidity': 30, 'dew_point': 7.56, 'wind_speed': 4.05, 'wind_deg': 250, 'wind_gust': 9.2, 'weather': [{'id': 802, 'main': 'Clouds', 'description': 'scattered clouds', 'icon': '03d'}], 'clouds': 44, 'pop': 0, 'uvi': 3}
    """
    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        avg_temp = d["temp"]["day"]
        min_temp = d["temp"]["min"]
        max_temp = d["temp"]["max"]

        ret.append("('{}',{},{},{})".format(day, avg_temp, min_temp, max_temp))

    cur = get_Redshift_connection()
    
    # create temp table
    create_sql = f"""BEGIN;
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    date date primary key,
                    temp float,
                    min_temp float,
                    max_temp float,
                    updated_date timestamp default GETDATE()
                );
                
                DROP TABLE IF EXISTS {schema}.temp_{table};
                
                CREATE TABLE IF NOT EXISTS {schema}.temp_{table} (
                    date date primary key,
                    temp float,
                    min_temp float,
                    max_temp float,
                    updated_date timestamp default GETDATE()
                );
                
                INSERT INTO {schema}.temp_{table}
                    SELECT * FROM {schema}.{table};"""

    logging.info(create_sql)
    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # Insert to temp table
    insert_sql = f"BEGIN; INSERT INTO {schema}.temp_{table} VALUES " + ",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    alter_sql = f"""BEGIN;
                    DELETE FROM {schema}.{table};
                    INSERT INTO {schema}.{table}
                    SELECT date, temp, min_temp, max_temp FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY updated_date DESC) seq
                        FROM {schema}.temp_{table}
                    )
                    WHERE seq = 1
                    ORDER BY date;"""
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

"""
CREATE TABLE choyoura.weather_forecast_inc (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    updated_date timestamp default GETDATE()
);
"""

dag = DAG(
    dag_id = 'Weather_Incremental',
    start_date = datetime(2021,9,9), 
    schedule_interval = '0 4 * * *', 
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

etl = PythonOperator(
    task_id = 'etl',
    python_callable = etl,
    # Seoul
    params = {
        "lat": 37.5665,
        "lon": 126.9780,
        "schema": "choyoura",
        "table": "weather_forecast_inc"
    },
    provide_context=True,
    dag = dag
)