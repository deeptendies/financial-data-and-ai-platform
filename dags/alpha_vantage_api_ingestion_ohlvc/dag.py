import os
from datetime import datetime
from datetime import timedelta

import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from dags.alpha_vantage_api_ingestion_ohlvc.callables import stock_data_ingestion

# pull connection from airflow backend
pg_conn = BaseHook.get_connection("deeptendies_postgres")
av_conn = BaseHook.get_connection("alpha_vantage_token_1")

# Defining pgURL
postgres_db = {'drivername': 'postgresql',
               'username': (pg_conn.login),
               'password': (pg_conn.password),
               'host': (pg_conn.host),
               'port': 5432,
               'database': 'deeptendies_sandbox'}
pgURL = URL(**postgres_db)

# pg engine
engine = create_engine(pgURL)


def callable(ticker):
    stock_data_ingestion()

# create dags dynamically
def create_dag(dag_id,
               schedule,
               config,
               default_args):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['dev', 'ingestion', 'alpha-vantage'],
              catchup=False)

    tickers = config['tickers']

    # generate one task per ticker
    with dag:
        for ticker in tickers:
            PythonOperator(
                task_id=f'alpha_vantage_api_ingestion_ohlvc_operator_{ticker}',
                python_callable=callable,
                op_kwargs={'ticker': ticker}
            )
    return dag


# reading configs from config.yml
pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

for config in dag_configs:
    dag_id = 'alpha_vantage_api_ingestion_ohlvc_{}'.format(str(config))
    default_args = {'owner': 'deeptendies',
                    'start_date': datetime(2021, 1, 1),
                    'retries': 5,
                    'retry_delay': timedelta(minutes=1),
                    }
    schedule = dag_configs[config]['schedule']
    globals()[dag_id] = create_dag(dag_id,
                                   schedule,
                                   dag_configs[config],
                                   default_args)
