import logging
import os
from datetime import datetime
from datetime import timedelta

import pandas_datareader as pdr
import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy.engine.url import URL
import pandas as pd

# pull connection from airflow
pg_conn = BaseHook.get_connection("deeptendies_postgres")

# Defining pgURL

postgres_db = {'drivername': 'postgresql',
               'username': (pg_conn.login),
               'password': (pg_conn.password),
               'host': (pg_conn.host),
               'port': 5432,
               'database': 'shared_sandbox'}
pgURL = URL(**postgres_db)

# pg engine
from sqlalchemy import create_engine

engine = create_engine(pgURL)

# auto arima libs
import pmdarima as pm
from pmdarima import model_selection
from pmdarima.arima import ndiffs
from pandas.tseries.offsets import *

import pandas as pd
def html_to_df(url):
    table=pd.read_html(url)
    df = table[0]
    return df

def ingest_html_operator(url):
    try:
        df = html_to_df(url)
    except:
        import pandas as pd
        import requests
        r = requests.get(url,headers ={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
        table = pd.read_html(r.text)
        df = table[0]
    return df

def execute(topic_name,
            url,
            schema="html_ingestion",
            *args, **kwargs):
    import re
    from datetime import date
    df = ingest_html_operator(url)
    try:
        df.columns = [re.sub(r'\W+', '', str(x)) for x in df.columns]
        print("sanitize columns succeed")
    except:
        print("sanitize columns failed")
        pass
    date = date.today().strftime("%Y_%m_%d")
    df.to_sql(name=f"{topic_name}_{date}",
              con=engine,
              schema=schema,
              if_exists='replace',
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

    url = config['url']

    # generate one task per ticker
    with dag:
            ops = PythonOperator(
                task_id=f'stock_data_ingestion_operator_{topic_name}',
                python_callable=execute,
                op_kwargs={'topic_name': topic_name,
                           'url': url}
            )
    return dag


# reading configs from config.yml
pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

    for config in dag_configs:
        dag_id = 'html_ingestion_{}'.format(str(config))
        default_args = {'owner': 'deeptendies',
                        'start_date': datetime(2021, 11, 1),
                        'retries': 3,
                        'retry_delay': timedelta(minutes=10),
                        }
        schedule = dag_configs[config]['schedule']
        globals()[dag_id] = create_dag(dag_id,
                                       schedule,
                                       dag_configs[config],
                                       default_args)
