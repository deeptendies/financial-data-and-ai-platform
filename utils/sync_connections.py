import pandas as pd
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.utils import db
from datetime import datetime
from yaml import dump

# ref https://gist.github.com/vshih/c486bef62d072f13ba21e327f094ec6f

def dump_connections(**kwargs):
    with db.create_session() as session:
        connections = session.query(Connection).all()
    conn_dict = {'airflow': {'connections': [
        {(column if column.startswith('conn_') else 'conn_' + column): getattr(connection, column) for column in
         ['conn_id', 'conn_type', 'schema', 'host', 'port', 'login', 'password', 'extra']} for connection in
        connections]}}
    conn_dump = dump(conn_dict, sort_keys=False)
    print(conn_dump)
    df = pd.DataFrame.from_dict(conn_dict['airflow']['connections'])
    print(df.head())


with DAG(
        dag_id='dump-connections',
        start_date=datetime.min,
        tags=['utils'],
        catchup=False
) as dag:
    PythonOperator(
        task_id='dump-connections',
        python_callable=dump_connections,
    )
