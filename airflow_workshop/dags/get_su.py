import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


headers = {'Content-Type': "application/json", 'Authorization': f"Bearer {Variable.get('access_token')}"}

target_fields = ['fak_id', 'fak_title', 'wp_list']

def get_structural_units():
    url_down = 'https://op.itmo.ru/api/record/structural/workprogram'
    page = requests.get(url_down, headers=headers)
    res = list(json.loads(page.text))
    for su in res:
        df = pd.DataFrame.from_dict(su)
        # превращаем последний столбец в json
        df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.su_wp', df.values, target_fields = target_fields)

with DAG(dag_id='get_su', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_structural_units',
    python_callable=get_structural_units
    )
 
t1 