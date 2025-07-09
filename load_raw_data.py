import pandas as pd
from sqlalchemy import text, create_engine
from jproperties import Properties
import numpy as np


trade_file_name = 'trades2.xlsx'     # the name of the file donwloaded from B3

id  = input('Type the client ID: ')
id = int(id)

p = Properties()

with open('app.properties', 'r+b') as f:

    p.load(f)

uri = p.get('uri').data

engine = create_engine(uri)


# read with pandas; 

df = pd.read_excel(trade_file_name, 
                 header = 0, engine = 'openpyxl')


try:
    with engine.connect() as conn:
        existent_dates = conn.execute(text("""
                        SELECT "Data do Negócio"
                            FROM
                        b3_raw_ops   
                        """)).fetchall()
        
    existent_dates = np.array(existent_dates).reshape(1, -1)[0]
    db_exists = True

except:
    print('DB not found')
    existent_dates = None
    db_exists = False


# check if any of the dates beeing uploaded already exists within the database;

if db_exists:
    assert len(
        np.intersect1d(df['Data do Negócio'].values, existent_dates)
        ) == 0, "Your data have insertecting dates within the database - please verify"


df['wallet_id'] = id


df.to_sql('b3_raw_ops', engine, if_exists='append', index = False) # injection
print('Data inserted sucessfully')

