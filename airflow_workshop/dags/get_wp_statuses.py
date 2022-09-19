import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

headers = {'Content-Type': "application/json", 'Authorization': f"Bearer {Variable.get('access_token')}"}

dt = pendulum.now().to_iso8601_string()

import datetime

def get_up(up_id):
    up_id = str(up_id)
    url = 'https://op.itmo.ru/api/workprogram/isu/' + up_id
    page = requests.get(url, headers=headers)

    try:
        df = pd.DataFrame([{**page.json(), "cop_id_up": up_id}])
        df = df.drop(['wp_url'], axis=1)

        requests.post(
            'http://192.168.0.105:13337/logUp',
            json={ "url": url, "response": page.json(), "created_at": datetime.datetime.now().isoformat() },
            headers = { 'Content-type': 'application/json' }
        )

        if len(df)>0:
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_status', df.values, target_fields=df.columns.tolist())
    except:
        ...

def get_wp_status():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select distinct cop_id_up from stg.wp_description
    """)
    for i in ids:
        get_up(i[0])

with DAG(dag_id='get_wp_statuses', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_wp_status',
    python_callable=get_wp_status
    )
 
t1 