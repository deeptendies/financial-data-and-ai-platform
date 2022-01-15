import logging
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pandas_datareader as pdr
import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy.engine.url import URL
import pandas as pd

# pull connection from airflow
pg_conn = BaseHook.get_connection("deeptendies_postgres")
fh_conn = BaseHook.get_connection("finnhub_token_1")
# Defining pgURL

postgres_db = {'drivername': 'postgresql',
               'username': (pg_conn.login),
               'password': (pg_conn.password),
               'host': (pg_conn.host),
               'port': 5432,
               'database': 'deeptendies_sandbox'}
pgURL = URL(**postgres_db)

# pg engine
from sqlalchemy import create_engine

engine = create_engine(pgURL)

try:
    import finnhub
except:
    import os
    os.system('pip install finnhub-python')
import pandas as pd


def execute(topic_name,
            schema="finnhub_api_ingestion",
            table_name = "earning_calendar",
            *args, **kwargs):
    import finnhub
    import datetime
    finnhub_client = finnhub.Client(api_key=fh_conn.password)
    today = datetime.date.today()
    next_4_weeks = (datetime.datetime.today() + datetime.timedelta(days=28)).date()
    api_res = finnhub_client.earnings_calendar(_from=today, to=next_4_weeks, symbol="", international=False)
    df = pd.json_normalize(api_res, max_level=1, record_path=['earningsCalendar'])

    column = 'date'
    try:
        dest = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{table_name}\"",
                           con=engine
                           )
    except:
        pass
    if 'dest' in locals():
        pd.concat([dest, df]).drop_duplicates.to_sql(name=topic_name,
                  con=engine,
                  schema=schema,
                  if_exists='replace',
                  index=False,
                  method='multi')
    else:
        df.to_sql(name=topic_name,
                  con=engine,
                  schema=schema,
                  if_exists='append',
                  index=False,
                  method='multi')


# create dags logic
def create_dag(topic_name,
               schedule,
               config,
               default_args):
    dag = DAG(topic_name,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['dev', 'data-ingestion', 'html-source'],
              catchup=False)

    # generate one task per ticker
    with dag:
        ops = PythonOperator(
            task_id=f'operator_{topic_name}',
            python_callable=execute,
            op_kwargs={'topic_name': topic_name}
        )
    return dag


# reading configs from config.yml
pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

    for config in dag_configs:
        dag_id = 'finnhub_api_ingestion_earning_calendar_{}'.format(str(config))
        default_args = {'owner': 'deeptendies',
                        'start_date': datetime(2021, 11, 1),
                        'retries': 3,
                        'retry_delay': timedelta(minutes=1),
                        }
        schedule = dag_configs[config]['schedule']
        globals()[dag_id] = create_dag(dag_id,
                                       schedule,
                                       dag_configs[config],
                                       default_args)
