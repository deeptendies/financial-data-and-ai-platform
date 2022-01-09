import logging
import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator


def import_pands():
    """
    example of dynamically install & import dependencies without rebuilding airflow
    :return:
    """
    try:
        import pandas
    except:
        os.system('pip install pandas')


def execute(dag_name,
            variable):
    """
    example executor method. For custom operator & executors, visit:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html#hooks
    :param variable:
    :param dag_name:
    :return:
    """
    logging.info(f"execute dag '{dag_name}'")
    logging.info(f"the passed variable is '{variable}'")
    return


def create_dag(dag_id,
               schedule,
               config,
               default_args):
    """
    example dynamic dag creator for creating dags from config
    :param dag_id:
    :param schedule:
    :param config:
    :param default_args:
    :return:
    """
    dag = DAG(f'dag_{dag_id}',
              schedule_interval=schedule,
              default_args=default_args,
              description='A starter DAG',
              tags=['starter', 'example'],
              catchup=False)

    with dag:
        task1 = PythonOperator(
            task_id=f'task_1_{dag_id}',
            python_callable=execute,
            op_kwargs={'dag_name': dag_id,
                       'variable': config['parameter_example']}
        )
        task2 = PythonOperator(
            task_id=f'task_2_{dag_id}',
            python_callable=execute,
            op_kwargs={'dag_name': dag_id,
                       'variable': config['parameter_example']}
        )
        task3 = PythonOperator(
            task_id=f'task_3_{dag_id}',
            python_callable=execute,
            op_kwargs={'dag_name': dag_id,
                       'variable': config['parameter_example']}
        )
        task4 = PythonOperator(
            task_id=f'task_4_{dag_id}',
            python_callable=execute,
            op_kwargs={'dag_name': dag_id,
                       'variable': config['parameter_example']}
        )
        task1 >> [task2, task3] >> task4
    return dag


def pipeline_init():
    """
    init the dynamic pipeline generation process
    :return:
    """
    pwd = os.path.split(__file__)[0]
    with open(os.path.join(pwd, "config.yml"), "r") as config_yaml:
        config_yml = yaml.load(config_yaml, Loader=yaml.FullLoader)
        for dag_config in config_yml:
            dag_id = 'starter_dag_{}'.format(str(dag_config))
            default_args = {'owner': 'airflow',
                            'start_date': datetime(2022, 1, 1),
                            'retries': 3,
                            'retry_delay': timedelta(minutes=1),
                            }
            schedule = config_yml[dag_config]['schedule']
            globals()[dag_id] = create_dag(dag_id,
                                           schedule,
                                           config_yml[dag_config],
                                           default_args)
import_pands()
pipeline_init()