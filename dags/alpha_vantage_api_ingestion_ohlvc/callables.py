import logging
import pandas_datareader as pdr # pip install pandas-datareader

def stock_data_ingestion(ticker, api_key=None, engine=None, *args, **kwargs):
    source = 'av-daily'
    start = '2021-01-01'
    df = pdr.data.DataReader(ticker.upper(),
                             data_source=source,
                             start=start,
                             api_key=api_key)
    logging.info(df.head())
    df.to_sql(name=ticker,
              con=engine,
              index=False,
              schema='alpha_vantage_api_ingestion_ohlvc',
              if_exists='replace')

if __name__ == '__main__':
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL
    postgres_db = {'drivername': 'postgresql',
                   'username': '(pg_conn.login)',
                   'password': '(pg_conn.password)',
                   'host': '(pg_conn.host)',
                   'port': 5432,
                   'database': 'deeptendies_sandbox'}
    pgURL = URL(**postgres_db)
    engine = create_engine(pgURL)

    stock_data_ingestion(ticker='NOK',
                         api_key='{{ #TODO: replace your API key}} for local testing',
                         engine=engine)