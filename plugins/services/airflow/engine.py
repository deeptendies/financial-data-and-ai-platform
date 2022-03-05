import sys

def deeptendies_engine():
    try:
        from airflow.hooks.base import BaseHook
        from sqlalchemy.engine.url import URL
        from sqlalchemy import create_engine
    except:
        print("Oops!", sys.exc_info()[0], "occurred.\n")
        print("Could not import airflow/sqlalchemy.")

    try:
        pg_conn = BaseHook.get_connection("deeptendies_postgres")
        postgres_db = {
            'drivername': 'postgresql',
            'username': (pg_conn.login),
            'password': (pg_conn.password),
            'host': (pg_conn.host),
            'port': 5432,
            'database': 'deeptendies_sandbox'
        }
        pgURL = URL(**postgres_db)
        engine = create_engine(pgURL)
        return engine
    except:
        print("Oops!", sys.exc_info()[0], "occurred.\n")
        print("Could not connect to the postgres server")
        return None
