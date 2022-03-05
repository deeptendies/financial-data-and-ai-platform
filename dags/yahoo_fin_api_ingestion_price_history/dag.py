import logging
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pandas.io.clipboards import to_clipboard

import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy.engine.url import URL
import pandas as pd
try:
    import yfinance as yf
except:
    os.system("pip install fyinance")
    import yfinance as yf

pg_conn = BaseHook.get_connection("deeptendies_postgres")
# yf_conn = BaseHook.get_connection("yahoo_fin_token_1")

postgres_db = {
    'drivername': 'postgresql',
    'username': (pg_conn.login),
    'password': (pg_conn.password),
    'host': (pg_conn.host),
    'port': 5432,
    'database': 'deeptendies_sandbox'
}
pgURL = URL(**postgres_db)

from sqlalchemy import create_engine
engine = create_engine(pgURL)

def get_hist_price_data(ticker, 
                        schema="yahoo_fin_histprice_api_ingestion", 
                        table_name="price_history",
                        *args, **kwargs):
    try:
        hist = yf.Ticker(ticker=ticker).history(period="max")
        hist["date"] = hist.index
        hist = hist.reset_index()
    except:
        pass
    try:
        dest = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{table_name}\"",
                           con=engine
                           )
    except:
        pass
    if 'dest' in locals():
        pd.concat([dest, hist]).drop_duplicates.to_sql(
            name=ticker,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi"
        )
    else:
        hist.to_sql(
                name=ticker,
                con=engine,
                index=False,
                schema=schema,
                if_exists='append',
                method="multi")

def create_dag(topic_name,
               schedule,
               config,
               default_args):
    dag = DAG(topic_name,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['dev', 'data-ingestion', 'html-source'],
              catchup=False)
    
    tickers = config['tickers']
    with dag:
        for ticker in tickers:
            PythonOperator(
                task_id=f"yahoo_fin_histprice_api_ingestion_operator_{ticker}",
                python_callable=get_hist_price_data,
                op_kwargs={'ticker': ticker}
            )
    return dag

pwd = os.path.split(__file__)[0]
with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
    dag_configs = yaml.load(config_yaml, Loader=yaml.FullLoader)

    for config in dag_configs:
        dag_id = f'yahoo_fin_histprice_api_ingestion_{str(config)}'
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