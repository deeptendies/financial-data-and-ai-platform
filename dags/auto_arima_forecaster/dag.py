import logging
import os
from datetime import datetime
from datetime import timedelta

import pandas as pd
import pandas_datareader as pdr
import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy.engine.url import URL

# pull connection from airflow
from dags.auto_arima_forecaster.callables import auto_arima

pg_conn = BaseHook.get_connection("deeptendies_postgres")

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

# auto arima libs
import pmdarima as pm
from pmdarima import model_selection
from pmdarima.arima import ndiffs
from pandas.tseries.offsets import *


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
                task_id=f'operator_{ticker}',
                python_callable=auto_arima,
                op_kwargs={'ticker': ticker,
                           'engine': engine}
            )
    return dag


# reading configs from config.yml
pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

for config in dag_configs:
    dag_id = 'auto_arima_forecaster_{}'.format(str(config))
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
