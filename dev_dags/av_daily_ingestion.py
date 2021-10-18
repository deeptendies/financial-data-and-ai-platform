# Ref
# https://www.astronomer.io/guides/dynamically-generating-dags
import logging
# Defining postgres variables
import os
from datetime import datetime, timedelta
import pandas_datareader as pdr
import yaml
from airflow import DAG
# getting connections from
# dev environment
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# pull connection vars
connection = BaseHook.get_connection("deeptendies_postgres")
POSTGRES_HOST = connection.host
POSTGRES_USER = connection.login
POSTGRES_PASSWORD = connection.password
av_conn = BaseHook.get_connection("alpha_vantage_token_1")
AV_API_KEY = av_conn.password

# Defining pgURL
from sqlalchemy.engine.url import URL

# https://www.pythonsheets.com/notes/python-sqlalchemy.html
postgres_db = {'drivername': 'postgresql',
               'username': POSTGRES_USER,
               'password': POSTGRES_PASSWORD,
               'host': POSTGRES_HOST,
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
        # ticker = ticker
        source = 'av-daily'
        start = '2021-01-01'
        end = '2021-05-01'
        df = pdr.data.DataReader(ticker.upper(),
                                 data_source=source,
                                 start=start,
                                 # end=end,
                                 api_key=AV_API_KEY)
        logging.info(df.head())
        df.to_sql(name=ticker,
                  con=engine,
                  schema='av_data_ingestion',
                  if_exists='replace')

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['dev'],
              catchup=False)

    tickers = config['tickers']

    with dag:
        for ticker in tickers:
            pyop = PythonOperator(
                task_id=f'stock_data_ingestion_operator_{ticker}',
                python_callable=execute_stock_data_ingestion,
                op_kwargs={'ticker': ticker}
            )
    return dag


# reading configs from config.yml
with open(os.path.join(os.path.split(__file__)[0], "config.yml"), "r") as yamlfile:
    dag_configs = yaml.load(yamlfile, Loader=yaml.FullLoader)

for config in dag_configs:
    dag_id = 'dag_stock_data_ingestion_{}'.format(str(config))
    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1),
                    'retries': 5,
                    'retry_delay': timedelta(minutes=1),
                    }
    schedule = dag_configs[config]['schedule']  # crontab: every 10 minutes
    globals()[dag_id] = create_dag(dag_id,
                                   schedule,
                                   dag_configs[config],
                                   default_args)
