import logging
import pandas_datareader as pdr # pip install pandas-datareader
from dags.alpha_vantage_api_ingestion_ohlvc.dag import av_conn, engine


def stock_data_ingestion(ticker, api_key=(av_conn.password), *args, **kwargs):
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
    stock_data_ingestion(ticker='NOK',
                         api_key='{{ #TODO: replace your API key}} for local testing')