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


def diff_stationalization(ticker,
                          schema = "av_data_ingestion",
                          *args, **kwargs):
    df = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{ticker}\"",
                     con=engine,
                     index_col='index')
    print(df.head())
    # feature engineer diff stationalization
    for i in df.columns:
        df[f'{i}_diff'] = df[[i]].diff()
    print(df.head())
    df.sort_index(inplace=True)
    df.to_sql(name=ticker,
              con=engine,
              schema='feature_engineering',
              if_exists='replace',
              method='multi')

# create dags logic
def create_dag(dag_id,
               schedule,
               config,
               default_args):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['dev', 'feature-engineering', 'etl'],
              catchup=False)

    tickers = config['tickers']

    # generate one task per ticker
    with dag:
        for ticker in tickers:
            PythonOperator(
                task_id=f'stock_data_ingestion_operator_{ticker}',
                python_callable=diff_stationalization,
                op_kwargs={'ticker': ticker}
            )
    return dag


# reading configs from config.yml
pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

for config in dag_configs:
    dag_id = 'diff_stationalization_{}'.format(str(config))
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