import os
from datetime import datetime
from datetime import timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.services.yahoo.yahoo_finance import get_hist_price_data


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
