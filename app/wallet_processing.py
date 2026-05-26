import pandas as pd
import numpy as np
from sqlalchemy import text
from jproperties import Properties
from datetime import timedelta
from app.db_conn import connect_db as cdb
from app.holidays import holidays
from datetime import timedelta 
import datetime as dt
from app.holidays import get_settlement_date
import logging
from decimal import Decimal

log = logging.getLogger(__name__)
logging.basicConfig(filename = 'app.log', level=logging.INFO)


### vamos identificar o usuario como id_carteira. Vamos quantificar com aportes para saber o cpaital aplicado e posuir um sistema de conta-corrente; 
# vamos tb separar por corretora todas as posições, ja que isso será essencial; 

# vamos começar como start_date 05/06/2025, dia que boletei a compra de 50 papeis de BBAS3; 

# por hora, tratar somente bolsa, marcando-a como renda variavle-bolsa. Depois, vamos permitir que a RF seja colocada junto;



db = cdb()


with db.engine.connect() as conn:

    data = conn.execute(text('SELECT * FROM int_b3_ops;')).fetchall()


df_ops = pd.DataFrame(data)
df_ops.loc[:, 'date'] = pd.to_datetime(df_ops.loc[:, 'date'])

df_ops.head(3)


### holidays and dates;

holidays = holidays()

# #### Feature Eng.


### need a settlement date (or liquidation - don't know the term, change after;

df_ops['lqd_date'] = df_ops.date + pd.Timedelta(days = 2)


for index, row in df_ops.iterrows():

    lqd_date = row.lqd_date

    while lqd_date in holidays or lqd_date.weekday in (5,6):
        lqd_date = lqd_date + timedelta(days = 1)

    df_ops.loc[index, 'lqd_date'] = lqd_date     # changes in locus;




class Wallet:

    def __init__(self, wallet_id):
        
        self.wallet_id = wallet_id
        self.db = cdb()
        self.engine = db.engine
        
    
    def get_position(self, date):
        
        with self.engine.connect() as conn:

            data = conn.execute(text(
            f"""
            SELECT 
            * 
            FROM wallet w 
            WHERE w.date = '{date}'
            AND wallet_id = {self.wallet_id}
            """)
            )

            self.position = pd.DataFrame(data.fetchall())
        
        return self.position


    def get_last_position(self):
        
        with self.engine.connect() as conn:

            data = conn.execute(text(
            f"""
            SELECT 
            * 
            FROM wallet w 
            WHERE w.date = (SELECT MAX(date) from wallet)
            AND w.wallet_id = {self.wallet_id}
            """)
            )
        
            self.last_position = pd.DataFrame(data.fetchall())

        return self.last_position
    
    def get_last_position_1dayb(self):
        
        with self.engine.connect() as conn:

            data = conn.execute(text(
            f"""
            SELECT 
            * 
            FROM wallet w 
            WHERE w.date = (SELECT MAX(date) -1 from wallet)
            AND w.wallet_id = {self.wallet_id}
            """)
            )
        
            self.last_position_d1 = pd.DataFrame(data.fetchall())

        return self.last_position_d1        


    def get_operations(self, date: str = '19901231'):
        
        """
        Parameters:
            date:str (optional) -> if not specified, will return all operations that are more recent than 1990-12-31
        """
        
        with self.engine.connect() as conn:
                
            data = conn.execute(text(
            f""" 
            SELECT *
            FROM 
            int_b3_ops ibo 
            where ibo.date >= '{date}'
            """
            ))

        self.df_op = pd.DataFrame(data.fetchall())
        
        return self.df_op
    

    def update_position(self, end_date: str = None):
        
        """
        Parameters:
            end_date: str 'yyyymmdd'
        """

        cashflow = Cashflow(self.wallet_id)

        # we need -> the last position and all the operations that were closed after that date.
        df_lp = self.get_last_position()
        
        # FILTER OPERATIONS AND SET UP START DATE AND END DATE FOR PROCESSING;
        if not df_lp.empty:
            last_processed_date = df_lp['date'].loc[0] # holds the latest date which a position snapshot exists.
            start_date = last_processed_date + timedelta(days=1)
            start_date_str = start_date.strftime('%Y%m%d')
            df_op = self.get_operations(start_date_str)

        else:
            df_op = self.get_operations()
            start_date = df_op.date.min()
        
        end_date = dt.datetime.strptime(end_date, '%Y%m%d').date()          

        if start_date > end_date:
            print('This date was already processed anytime before. Exiting...')
            return None

        # DATE RANGE FOR PROCESSING
        op_date_range = pd.date_range(start_date, end_date, freq='D')
        op_date_range = [date.date() for date in op_date_range] # transform timestamp in datetime.date

        # LOOP THROUGH DATE INTERVAL
        for date in op_date_range:

            df_lp = self.get_last_position()    # for every loop, refresh our position.
            
            if not df_lp.empty: # start clonning the last position
                df_lp.date = date
                df_lp.to_sql(name = 'wallet', con = self.engine, schema='public', if_exists='append', index = False)

            if df_op.empty:  # skip, lp was clonned to the current date
                print(f'{date}, D-1 position clonned')
                next

            else:
                # FILTER OPs BASED ON CURRENT DATE OF PROCESSING 
                df_op_temp = df_op[df_op.date == date] # sub dataframe; 

                for index, row in df_op_temp.iterrows():

                    op_id = row.id
                    op_asset = row.asset
                    op_qtty = row.quantity
                    op_value = row.total_amount
                    op_broker = row.brokerage_firm_id
                    op_wallet_id = row.wallet_id
                    op_movement = row.event_type
                    op_debcred = row.movement

                    log.info(f'Row ID beeing processed from int_b3_ops (c1): {op_id}')
                    
                    # Signal ajustm. for debt op.
                    if op_debcred == 'Debito':      
                        op_qtty = -op_qtty 


                    # real loop starts here. 

                    # PROVENTOS
                    if row.event_type in ("JCP", "Dividendo"):
                        cashflow.trade_settlement(
                            1011,9,101, 1, date, op_value, op_id, op_debcred, op_movement
                        )
                        continue


                    if row.event_type not in ("Compra", "Venda", "Transferência - Liquidação", "Transferência", "Desdobro"):
                        continue # must be finished to treat split etc.


                    # Finance involved with the operation
                    if row.event_type in ("Compra", "Venda", "Transferência - Liquidação"):
                        
                        cashflow.trade_settlement(
                            1011,9,101, 1, date, op_value, op_id, op_debcred, op_movement
                        )


                    df_lp= self.get_last_position() # refresh
                    
                    if df_lp.empty:  # means its running for the first time. So, just put the first row as a position;
                        
                        print('First time running the code - first row will be added directly to the wallet')
                        df = pd.DataFrame(row).T[['date', 'wallet_id', 'brokerage_firm_id', 'asset', 'quantity', 'pu', 'total_amount']]
                        df.to_sql('wallet', self.engine, if_exists = 'append', schema = 'public', index = False)
                        next

                    else: # 2 options - new asset or update existing asset
                        
                        # option 1 - update asset;
                        triad_of_values = (op_broker, op_asset, op_wallet_id)
                        array_of_exsisting_values = df_lp.loc[:, ['brokerage_firm_id', 'asset', 'wallet_id']].values
                        
                        if any(tuple(x)==triad_of_values for x in array_of_exsisting_values) :

                            print(f'asset {op_asset} under position, updating -- {date} - {op_asset}')
                            
                            df_lp_filt = df_lp.where(
                                (df_lp.brokerage_firm_id == op_broker) & (df_lp.asset == op_asset) & (df_lp.wallet_id == op_wallet_id)
                            ).dropna()
                            
                            assert df_lp_filt.empty == False, f"{row}\n{op_broker}, {op_asset}, {op_wallet_id}"

                            old_qtty = df_lp_filt.quantity.iloc[0]
                            old_value = df_lp_filt.total_amount.iloc[0]

                            # new values for quantity, avg price and value
                            new_qtty = old_qtty + op_qtty

               
                            if new_qtty != 0:
                                
                                if op_movement == 'Compra' or op_movement == 'Venda' or op_movement == 'Transferência - Liquidação':
                                
                                    if op_debcred == 'Credito':
                                        new_value = df_lp_filt.total_amount.iloc[0] + op_value
                                        new_pu = new_value / new_qtty
                                
                                    elif op_debcred == 'Debito':
                                        new_pu = df_lp_filt.pu.iloc[0] # same as before;
                                        new_value = new_qtty * new_pu

                                
                                # IF IS CUSTODY TRANSFER
                                if op_movement == "Transferência":

                                    if op_debcred == 'Debito':
                                        new_pu = df_lp_filt.pu.iloc[0] # same as before;
                                        new_value = new_qtty * new_pu      

                                    elif op_debcred == 'Credito':

                                        # need to get the origin from which the stocks came. As the deb and credit are accounted in the same day with the STVM, we can find the equivalent counterpart of this STVM op.
                                        origem_firm_id = df_op_temp[
                                            (df_op_temp.movement == 'Debito') & 
                                            (df_op_temp.asset == op_asset) & 
                                            (df_op_temp.quantity == op_qtty) &
                                            (df_op_temp.event_type == "Transferência")
                                            ].iloc[0]['brokerage_firm_id']
                                        
                                        df_lp_filt_4stvm = df_lp.where(
                                            (df_lp.brokerage_firm_id == origem_firm_id) & (df_lp.asset == op_asset) & (df_lp.wallet_id == op_wallet_id)
                                        ).dropna()
                                        assert df_lp_filt_4stvm.empty == False, print(f'Assert fail: df4stvm print:\n {df_lp_filt_4stvm}')

                                        new_value = Decimal(df_lp_filt_4stvm.total_amount.iloc[0]) + df_lp_filt.total_amount.iloc[0]
                                        new_pu = new_value / new_qtty
                                
                                elif op_movement == 'Desdobro':
                                    
                                    proportion = new_qtty / old_qtty
                                    new_value = old_value 
                                    new_pu = new_value / new_qtty

                            
                            # LIQUIDATION OF POSITION
                            elif new_qtty == 0:
                                new_pu = 0
                                new_value = 0
                            
                                
                            # DATABASE UPDATE
                            with self.engine.connect() as conn:
                                
                                query = text(
                                     f"""
                                    UPDATE wallet
                                    SET 
                                        quantity = {new_qtty},
                                        total_amount = {new_value},
                                        pu = {new_pu}
                                    WHERE 
                                        asset = '{op_asset}' and brokerage_firm_id = {op_broker} and date = '{date.strftime('%Y-%m-%d')}'
                                    """
                                )
                                conn.execute(query)
                                conn.commit()
                            print(f'{date} - {op_asset}, position updated')

                        else: # option 2 - add new asset;

                            if op_movement == 'Compra' or op_movement == 'Venda' or op_movement == 'Transferência - Liquidação':

                                df = pd.DataFrame(row).T[['date', 'wallet_id', 'brokerage_firm_id', 'asset', 'quantity', 'pu', 'total_amount']]
                                df.to_sql('wallet', self.engine, if_exists = 'append', schema = 'public', index = False)    
                                print(f'new asset inserted -- {date} - {op_asset}')

                            elif op_movement == "Transferência":
                                # got to retrive from wallet infos;

                                df_lp_d1 = self.get_last_position_1dayb()

                                origem_firm_id = df_op_temp[
                                    (df_op_temp.movement == 'Debito') & 
                                    (df_op_temp.asset == op_asset) & 
                                    (df_op_temp.quantity == op_qtty) &
                                    (df_op_temp.event_type == "Transferência")
                                    ].iloc[0]['brokerage_firm_id']
                                
                                df_lp_filt_4stvm = df_lp_d1.where(
                                    (df_lp_d1.brokerage_firm_id == origem_firm_id) & (df_lp_d1.asset == op_asset) & (df_lp_d1.wallet_id == op_wallet_id)
                                ).dropna()     
                                assert df_lp_filt_4stvm.empty == False, print(f'Assert fail: df4stvm print:\n {df_lp_filt_4stvm}')

                                pu_origem =     df_lp_filt_4stvm.pu.iloc[0]     
                                total_amount_origem =   df_lp_filt_4stvm.total_amount.iloc[0]                        

                                df = pd.DataFrame(
                                    data = [[date, op_wallet_id, op_broker, op_asset, op_qtty, pu_origem, total_amount_origem]],
                                    columns = ['date', 'wallet_id', 'brokerage_firm_id', 'asset', 'quantity', 'pu', 'total_amount'])
                                df.to_sql('wallet', self.engine, if_exists = 'append', schema = 'public', index = False)    
                                print(f'new asset inserted -- {date} - {op_asset}')
                                

                    
                    df_lp = self.get_last_position()    # everytime a op is processed, we need to refresh LP

            
        return None


class Cashflow:

    def __init__(self, wallet_id: int):

        self.wallet_id = wallet_id
        self.db = cdb()
        self.engine = db.engine


    def get_transaction_history(self, end_date: str, start_date: str = None) -> pd.DataFrame:

        """
        Parameters:
            start_date (str, optional): format 'YYYYMMDD'
            end_date (str): format 'YYYYMMDD'
        """
        
        if start_date == None:
            start_date = end_date

        with self.engine.connect() as conn:
            query = f"""
            SELECT * FROM 
            cashflow
            WHERE date between '{start_date}' AND '{end_date}'
            """

            data = conn.execute(text(query)).fetchall()
            df = pd.DataFrame(data)

        
        if df.empty:
            return print("There is no recorded data in this table.")

        return df
    

    def trade_settlement(self, account: int, vd: int, bank_id: int, agency: int, date: dt.date, op_value: float, op_id: int, op_debcred, op_event):
        """
        
        This function is used to register the ins and outs for the bank account related only to operations - buys and sells. 
        Parameters:
            df (DataFrame): the dataframe with the operation's data.
        
        """

        # the settlement date is D+2 from the operation date - and only working dates - so we need to adjust; 
        
        op_settlement_date = get_settlement_date(date)

        # for events that consumes CASH
        if op_debcred == 'Credito' and op_event == 'Transferência - Liquidação':
            
            final_value = -1* op_value # oposite 
        else:
            final_value = op_value
       

        with self.engine.connect() as conn:

            query = f"""
            INSERT INTO 
            cashflow (
            wallet_id, account, vd, bank_id, agency, date, total_amount, origem_id) 
            VALUES (
            {self.wallet_id}, {account}, {vd}, {bank_id}, {agency}, '{op_settlement_date}', {final_value}, {op_id})
            """

            conn.execute(text(query))
            conn.commit()
        
        return None
    

    

    def cash_transfer(self, account: int, vd: int, bank_id: int, agency: int, date: str, value: float, origem_id = 'NULL'):
        
        
        with self.engine.connect() as conn:
            query = f"""
                INSERT INTO cashflow (
                wallet_id, account, vd, bank_id, agency, date, total_amount, origem_id) 
                VALUES (
                {self.wallet_id}, {account}, {vd}, {bank_id}, {agency}, '{date}', {value}, {origem_id})
                """       
            
            conn.execute(text(query))
            conn.commit()
            
        
        return None
        


