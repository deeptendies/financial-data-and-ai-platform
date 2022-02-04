import pandas as pd
import pmdarima as pm
from pandas.tseries.offsets import __all__
from pmdarima.arima import ndiffs


def auto_arima(ticker,
               schema="feature_engineering",
               engine=None,
               *args, **kwargs):
    df = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{ticker}\"",
                     con=engine,
                     # index_col='index'
                     )
    df['index'] = pd.to_datetime(df['index'])
    new_index = (df['index'] + BDay(14))[-14:]

    df_temp_1 = pd.DataFrame(new_index,
                             columns=['index'])
    for metric in ['high_diff', 'low_diff']:
        data = df[metric].dropna()
        kpss_diffs = ndiffs(data, alpha=0.05, test='kpss', max_d=6)
        adf_diffs = ndiffs(data, alpha=0.05, test='adf', max_d=6)
        n_diffs = max(adf_diffs, kpss_diffs)

        arima = pm.auto_arima(y=data.dropna(),
                              start_p=0, d=1, start_q=0,
                              max_p=5, max_d=5, max_q=5, start_P=0,
                              D=1, start_Q=0, max_P=5, max_D=5,
                              max_Q=5, m=12, seasonal=True,
                              error_action='warn', trace=True,
                              supress_warnings=True, stepwise=True,
                              random_state=20, n_fits=50
                              )
        new_data = arima.predict(14)
        df_temp_2 = pd.DataFrame(list(zip(new_index, new_data)),
                                 columns=['index', f'{metric}_forecast'])
        # print(df_temp_2.head())

        df_temp_1 = pd.merge(df_temp_1, df_temp_2, on='index')
        print(df_temp_1)
    df_temp_1.to_sql(name=ticker,
                     con=engine,
                     schema='auto_arima_forecasts',
                     if_exists='replace',
                     method='multi')