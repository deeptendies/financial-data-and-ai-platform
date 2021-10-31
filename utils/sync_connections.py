import pandas as pd
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.utils import db
from airflow.hooks.base import BaseHook
from datetime import datetime
from yaml import dump
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

# ref https://gist.github.com/vshih/c486bef62d072f13ba21e327f094ec6f
pg_conn = BaseHook.get_connection("deeptendies_postgres")
postgres_db = {'drivername': 'postgresql',
               'username': (pg_conn.login),
               'password': (pg_conn.password),
               'host': (pg_conn.host),
               'port': 5432,
               'database': 'admin'}
pgURL = URL(**postgres_db)
# pg engine
engine = create_engine(pgURL)


def dump_connections(**kwargs):
    with db.create_session() as session:
        connections = session.query(Connection).all()
    conn_dict = {'airflow': {'connections': [
        {(column if column.startswith('conn_') else 'conn_' + column): getattr(connection, column) for column in
         ['conn_id', 'conn_type', 'schema', 'host', 'port', 'login', 'password', 'extra']} for connection in
        connections]}}
    # conn_dump = dump(conn_dict, sort_keys=False)
    df = pd.DataFrame.from_dict(conn_dict['airflow']['connections'])
    # print(conn_dump)
    schema = 'connections'
    table_name = "airflow_connection_backup"
    dest = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{table_name}\"",
                       con=engine
                       )
    print(df.head())
    print(dest.head())
    df[~df.conn_id.isin(dest.conn_id)].to_sql(name=table_name,
              con=engine,
              schema=schema,
              if_exists='append')


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
