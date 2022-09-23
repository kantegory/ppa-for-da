import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

def parse_file(file_path):
    required_fields = [
        "plan_type",
        "direction_id",
        "ns_id",
        "direction_code",
        "direction_name",
        "edu_program_id",
        "edu_program_name",
        "faculty_id",
        "faculty_name",
        "training_period",
        "university_partner",
        "up_country",
        "lang",
        "military_department",
        "total_intensity",
        "ognp_id",
        "ognp_name",
        "selection_year",
    ]
    
    file = requests.get(f'http://192.168.1.38:13338/{file_path}')
    json_data = {**file.json(), 'disciplines_blocks': None}

    print("[UP DESCRIPTION] Parse file:", file_path)

    is_all_required_fields = all([
        field in list(json_data.keys())
        for field in required_fields
    ])

    if not is_all_required_fields:
        print("[UP DESCRIPTION] Error: skipping file, not all required fields", file_path)

        return pd.DataFrame()

    df = pd.DataFrame([json_data])
    df = df.drop(['disciplines_blocks'], axis=1)

    return df


def get_up_description():
    paths = [
        "10425_old.json",  "11719_old.json",  "13317_old.json",  "13373_old.json",  "14558_old.json",  "14657_old.json",
        "10426_old.json",  "11721_old.json",  "13318_old.json",  "13374_old.json",  "14559_old.json",  "14658_old.json",
        "10555_old.json",  "11723_old.json",  "13319_old.json",  "13375_old.json",  "14560_old.json",  "14659_old.json",
        "10556_old.json",  "11726_old.json",  "13320_old.json",  "13376_old.json",  "14561_old.json",  "14660_old.json",
        "10557_old.json",  "11729_old.json",  "13321_old.json",  "13377_old.json",  "14562_old.json",  "14661.json",
        "10558_old.json",  "11760_old.json",  "13322_old.json",  "13378_old.json",  "14563_old.json",  "14661_old.json",
        "10559_old.json",  "11792_old.json",  "13323_old.json",  "13379_old.json",  "14564_old.json",  "14662_old.json",
        "10560_old.json",  "12977_old.json",  "13324_old.json",  "13380_old.json",  "14565_old.json",  "14663_old.json",
        "10561_old.json",  "12997_old.json",  "13325_old.json",  "13381_old.json",  "14566_old.json",  "14664_old.json",
        "10562_old.json",  "13257_old.json",  "13326_old.json",  "13382_old.json",  "14567_old.json",  "14665_old.json",
        "10563_old.json",  "13258_old.json",  "13327_old.json",  "13383_old.json",  "14568_old.json",  "14666_old.json",
        "10564_old.json",  "13259_old.json",  "13328_old.json",  "13397_old.json",  "14569_old.json",  "14667_old.json",
        "10565_old.json",  "13260_old.json",  "13329_old.json",  "13398_old.json",  "14570_old.json",  "14668_old.json",
        "10566_old.json",  "13261_old.json",  "13330_old.json",  "13857_old.json",  "14571_old.json",  "14669_old.json",
        "10568_old.json",  "13262_old.json",  "13331_old.json",  "13859_old.json",  "14572_old.json",  "14670_old.json",
        "10569_old.json",  "13263_old.json",  "13332_old.json",  "13860_old.json",  "14573_old.json",  "14671_old.json",
        "10570_old.json",  "13264_old.json",  "13333_old.json",  "13861_old.json",  "14574_old.json",  "14672_old.json",
        "10571_old.json",  "13265_old.json",  "13334_old.json",  "14297_old.json",  "14575_old.json",  "14673_old.json",
        "10572_old.json",  "13266_old.json",  "13335_old.json",  "14517_old.json",  "14576_old.json",  "14677_old.json",
        "10573_old.json",  "13267_old.json",  "13336_old.json",  "14518_old.json",  "14577_old.json",  "14678_old.json",
        "10574_old.json",  "13268_old.json",  "13337_old.json",  "14519_old.json",  "14578_old.json",  "14679_old.json",
        "10575_old.json",  "13269_old.json",  "13338_old.json",  "14522_old.json",  "14579_old.json",  "14680_old.json",
        "10576_old.json",  "13270_old.json",  "13339_old.json",  "14523_old.json",  "14580_old.json",  "14681_old.json",
        "10577_old.json",  "13271_old.json",  "13340_old.json",  "14524_old.json",  "14581_old.json",  "14777_old.json",
        "10578_old.json",  "13272_old.json",  "13341_old.json",  "14525_old.json",  "14582_old.json",  "14897_old.json",
        "10579_old.json",  "13273_old.json",  "13342_old.json",  "14526_old.json",  "14583_old.json",  "14898_old.json",
        "10580_old.json",  "13274_old.json",  "13343_old.json",  "14527.json",      "14584_old.json",  "14899_old.json",
        "10581_old.json",  "13275_old.json",  "13344_old.json",  "14527_old.json",  "14585_old.json",  "15237_old.json",
        "10752_old.json",  "13276_old.json",  "13345_old.json",  "14528_old.json",  "14587_old.json",  "15297_old.json",
        "11676_old.json",  "13277_old.json",  "13346_old.json",  "14529_old.json",  "14588_old.json",  "15317_old.json",
        "11677_old.json",  "13278_old.json",  "13347_old.json",  "14530_old.json",  "14589_old.json",  "15517_old.json",
        "11678_old.json",  "13279_old.json",  "13348_old.json",  "14531_old.json",  "14590_old.json",  "15841.json",
        "11679_old.json",  "13280_old.json",  "13349_old.json",  "14532_old.json",  "14591_old.json",  "16009.json",
        "11680_old.json",  "13281_old.json",  "13350_old.json",  "14533_old.json",  "14592_old.json",  "16084.json",
        "11683_old.json",  "13282_old.json",  "13351_old.json",  "14534_old.json",  "14593_old.json",  "16086.json",
        "11685_old.json",  "13283_old.json",  "13352_old.json",  "14535_old.json",  "14594_old.json",  "16105.json",
        "11687_old.json",  "13297_old.json",  "13353_old.json",  "14536_old.json",  "14595_old.json",  "16114.json",
        "11689_old.json",  "13298_old.json",  "13354_old.json",  "14537_old.json",  "14596_old.json",  "17660.json",
        "11690_old.json",  "13299_old.json",  "13355_old.json",  "14538_old.json",  "14597_old.json",  "17663.json",
        "11692_old.json",  "13300_old.json",  "13356_old.json",  "14539_old.json",  "14598_old.json",  "17666.json",
        "11693_old.json",  "13301_old.json",  "13357_old.json",  "14540_old.json",  "14599_old.json",  "17670.json",
        "11695_old.json",  "13302_old.json",  "13358_old.json",  "14541_old.json",  "14600_old.json",  "17674.json",
        "11696_old.json",  "13303_old.json",  "13359_old.json",  "14542.json",      "14601_old.json",  "17708.json",
        "11698_old.json",  "13304_old.json",  "13360_old.json",  "14542_old.json",  "14602_old.json",  "17712.json",
        "11699_old.json",  "13305_old.json",  "13361_old.json",  "14543_old.json",  "14603_old.json",  "17722.json",
        "11701_old.json",  "13306_old.json",  "13362_old.json",  "14544_old.json",  "14604_old.json",  "17727.json",
        "11702_old.json",  "13307_old.json",  "13363_old.json",  "14545_old.json",  "14605_old.json",  "17734.json",
        "11703_old.json",  "13308_old.json",  "13364_old.json",  "14546_old.json",  "14606_old.json",  "9837_old.json",
        "11705_old.json",  "13309_old.json",  "13365_old.json",  "14547_old.json",  "14607_old.json",  "9859_old.json",
        "11707_old.json",  "13310_old.json",  "13366_old.json",  "14548_old.json",  "14637_old.json",  "9863_old.json",
        "11710_old.json",  "13311_old.json",  "13367_old.json",  "14549_old.json",  "14638_old.json",  "9868_old.json",
        "11711_old.json",  "13312_old.json",  "13368_old.json",  "14550_old.json",  "14639_old.json",  "9869_old.json",
        "11713_old.json",  "13313_old.json",  "13369_old.json",  "14551_old.json",  "14640_old.json",  "9871_old.json",
        "11715.json",      "13314_old.json",  "13370_old.json",  "14552_old.json",  "14641_old.json",  "9872_old.json",
        "11715_old.json",  "13315_old.json",  "13371_old.json",  "14553_old.json",  "14642_old.json",  "9874_old.json",
        "11717_old.json",  "13316_old.json",  "13372_old.json",  "14557_old.json",  "14644_old.json"
    ]

    for path in paths:
        df = parse_file(path)

        if df.empty:
            continue

        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows(
            'stg.up_description', df.values, target_fields=df.columns.tolist(), replace=True, replace_index='id')


with DAG(dag_id='get_up_descriptions', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@monthly",
         catchup=False) as dag:
    t1 = PythonOperator(
        task_id='get_up_descriptions',
        python_callable=get_up_description
    )

t1
