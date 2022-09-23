import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
headers = {'Content-Type': "application/json", 'Authorization': f"Bearer {Variable.get('access_token')}"}


def get_wp_descriptions():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.work_programs  restart identity cascade;
    """)
    target_fields = ['id', 'academic_plan_in_field_of_study', 'wp_in_academic_plan', 'update_ts']
    url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    for p in range(1, c // 10 + 2):
        url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=' + str(p)
        dt = pendulum.now().to_iso8601_string()
        page = requests.get(url_down, headers=headers)
        res = json.loads(page.text)['results']

        print('[WP DESCRIPTION] Current url:', url_down)
        print('[WP DESCRIPTION] Results:', res)

        for r in res:
            df = pd.DataFrame([r], columns=r.keys())
            df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
            df['wp_in_academic_plan'] = df[~df['wp_in_academic_plan'].isna()]["wp_in_academic_plan"].apply(lambda st_dict: json.dumps(st_dict))
            df.loc[:, 'update_ts'] = dt
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.work_programs', df.values, target_fields = target_fields)

def get_structural_units():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.su_wp  restart identity cascade;
    """)
    url_down = 'https://op.itmo.ru/api/record/structural/workprogram'
    target_fields = ['fak_id', 'fak_title', 'wp_list']
    page = requests.get(url_down, headers=headers)
    res = list(json.loads(page.text))

    print('[SU] Current url:', url_down)
    print('[SU] Results:', res)

    for su in res:
        df = pd.DataFrame.from_dict(su)
        # превращаем последний столбец в json
        df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.su_wp', df.values, target_fields = target_fields)


with DAG(dag_id='get_data', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_wp_descriptions',
    python_callable=get_wp_descriptions
    )
    t2 = PythonOperator(
    task_id='get_structural_units',
    python_callable=get_structural_units
    )

t1 >> t2
