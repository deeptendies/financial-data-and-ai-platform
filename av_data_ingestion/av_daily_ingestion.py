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

# pull connection from airflow
pg_conn = BaseHook.get_connection("deeptendies_postgres")
av_conn = BaseHook.get_connection("alpha_vantage_token_1")

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


# create dags logic
def create_dag(dag_id,
               schedule,
               config,
               default_args):
    def execute_stock_data_ingestion(ticker, *args, **kwargs):
        source = 'av-daily'
        start = '2021-01-01'
        df = pdr.data.DataReader(ticker.upper(),
                                 data_source=source,
                                 start=start,
                                 api_key=(av_conn.password))
        logging.info(df.head())
        df.to_sql(name=ticker,
                  con=engine,
                  schema='av_data_ingestion',
                  if_exists='replace')

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
                task_id=f'stock_data_ingestion_operator_{ticker}',
                python_callable=execute_stock_data_ingestion,
                op_kwargs={'ticker': ticker}
            )
    return dag


# reading configs from config.yml
pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

for config in dag_configs:
    dag_id = 'av_ingest_{}'.format(str(config))
    default_args = {'owner': 'apecrews',
                    'start_date': datetime(2021, 1, 1),
                    'retries': 5,
                    'retry_delay': timedelta(minutes=1),
                    }
    schedule = dag_configs[config]['schedule']
    globals()[dag_id] = create_dag(dag_id,
                                   schedule,
                                   dag_configs[config],
                                   default_args)