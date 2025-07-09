#%%
import pandas as pd
import numpy as np
from datetime import timedelta



def holidays():

    path = './app/feriados_nacionais.xls'

    df = pd.read_excel(path)

    df.loc[:, 'Data'] = pd.to_datetime(df.Data, errors = 'coerce')

    df = df.dropna(subset='Data')

    holidays_array = [x.date() for x in df.Data.values] # conmvert from timestamp to datetime.date


    return holidays_array



def get_settlement_date(date, holidays=holidays()):

    settle_date = date + timedelta(2)

    while settle_date in holidays or settle_date.weekday() in(5,6):
        settle_date+= timedelta(days=1)

    return settle_date




if __name__ == '__main__':
    
    holidays()

    
# %%
