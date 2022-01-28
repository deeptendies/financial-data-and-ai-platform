import os
import pandas as pd
from services.airflow.engine import deeptendies_engine
try:
    import yfinance as yf
except:
    os.system("pip install yfinance")
    import yfinance as yf


def get_hist_price_data(ticker,
                        schema="yahoo_fin_histprice_api_ingestion",
                        table_name="price_history",
                        *args, **kwargs):
    try:
        hist = yf.Ticker(ticker=ticker).history(period="max")
        hist["date"] = hist.index
        hist = hist.reset_index()
    except:
        print("Could not read yfinance")
        return
    try:
        engine = deeptendies_engine()
        dest = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{table_name}\"",
                           con=engine
                           )
    except:
        print("Engine failed")
        return
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
