#%%
import pandas as pd
import numpy as np



def holidays():

    path = './app/feriados_nacionais.xls'


    df = pd.read_excel(path)

    df.loc[:, 'Data'] = pd.to_datetime(df.Data, errors = 'coerce')

    df = df.dropna(subset='Data')

    return df.Data.values


    # mask = []

    # for index, row in df.iterrows():
    #     try:
    #         np.datetime64(row.Data)
    #         mask.append(True)
    #     except:
    #         mask.append(False)

    # df = df[mask]

    # return df.Data.values



if __name__ == '__main__':
    
    holidays()

    
# %%
