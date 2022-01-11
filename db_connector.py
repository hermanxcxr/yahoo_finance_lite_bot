import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import json
import os
import re
from datetime import datetime

class DbConnector:


    def __init__(self):
        with open('inputs/db_info.json','r',encoding='utf-8') as f:
            self.db_info = json.load(f)
    
    def db_info_data(self):
        return self.db_info
    
    def connector_zero(self):
        conn_sql = psycopg2.connect( user = self.db_info['user'],
                           password = self.db_info['password'],
                           port = self.db_info['port'],
                           )
        
        cursor = conn_sql.cursor()        
        conn_sql.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        '''
        sql_kill_sesions = """SELECT *, pg_terminate_backend(pid)
                        FROM pg_stat_activity 
                        WHERE pid <> pg_backend_pid()
                        AND datname = "finanzas_lite_db";"""
        cursor.execute(sql_kill_sesions)
        conn_sql.commit()
        '''

        sqlDeleteDatabase = 'drop database finanzas_lite_db --if-exists'
        cursor.execute(sqlDeleteDatabase)
        conn_sql.commit()
        

        sqlCreateDatabase = 'create database "finanzas_lite_db"'
        cursor.execute(sqlCreateDatabase)
        conn_sql.commit()

        conn_sql = psycopg2.connect( user = self.db_info['user'],
                           password = self.db_info['password'],
                           port = self.db_info['port'],
                           database = "finanzas_lite_db"
                           )
        cursor = conn_sql.cursor()

        creation_tickers_table = """create table tickers(
            TICKER_ID SERIAL NOT NULL PRIMARY KEY, 
            TICKER varchar(10) NOT NULL, 
            TICKER_NAME varchar(256));"""
        cursor.execute(creation_tickers_table)
        conn_sql.commit()

        with open('inputs/indexes.json','r',encoding='utf-8') as f:
            indexes = json.load(f)
        with open('inputs/exogenous_indexes.json','r',encoding='utf-8') as f:
            exo_indexes = json.load(f)
        
        idxs = indexes.copy()
        idxs.update(exo_indexes)

        idx_tables = """ create table {}_close_prices(
            SPC_ID SERIAL NOT NULL, 
            DATE DATE NOT NULL, 
            CLOSE_PRICE NUMERIC(9,3),
            TICKER_ID INT,
            PRIMARY KEY(SPC_ID),
            CONSTRAINT fk_tickers
                FOREIGN KEY(TICKER_ID)
                    REFERENCES tickers(TICKER_ID)
            );"""

        for key,value in idxs.items():
            cursor.execute(idx_tables.format(key))
        conn_sql.commit()

        with open('outputs/idxs_names.json','r',encoding='utf-8') as f:
            indexes_names = json.load(f)
        
        insert_idx_values = """ insert into tickers (ticker, ticker_name)
            values(
            '{}', '{}'
            );"""
        
        idx_id_dict = {}
        iteration =1
        for key,value in idxs.items():
            cursor.execute(insert_idx_values.format(idxs[key],indexes_names[key]))
            idx_id_dict[key] = iteration
            iteration +=1 
        conn_sql.commit()

        files_list = os.listdir('outputs/')
        idx_files_list = []
        for file in files_list:
            target = re.search(r'_close_prices',file)
            if target:
                idx_files_list.append(file)

        my_data = {}
        for file in idx_files_list:
            with open('outputs/{}'.format(file),'r',encoding='utf-8') as f:
                name = file.replace(".json","")
                my_data[name] = json.load(f)

        insert_idx_table_values = """ insert into {} (date, close_price, ticker_id)
            values(
            '{}', '{}', '{}'
            );"""
        
        for table_name,prices_dict in my_data.items():
            pattern = r','
            prices = {datetime.strptime(re.sub(pattern,'',key), '%b %d %Y') : float(re.sub(pattern,'',value)) 
                    for key,value in prices_dict.items() if value}
            ticker = table_name.replace("_close_prices","")
            ticker_id = idx_id_dict[ticker]
            
            for key,value in sorted(prices.items()):
                cursor.execute(insert_idx_table_values.format(table_name,key,value,ticker_id))
        conn_sql.commit()

        cursor.close()
        conn_sql.close()

    def connector_reg(self):
        conn_sql = psycopg2.connect( user = self.db_info['user'],
                           password = self.db_info['password'],
                           port = self.db_info['port'],
                           database = "finanzas_lite_db"
                           )
        
        cursor = conn_sql.cursor()

        with open('outputs/ultimate_data.json','r',encoding='utf-8') as f:
            ultimate_data = json.load(f)
        
        insert_idx_table_values = """ insert into {}_close_prices (date, close_price, ticker_id)
            values(
            '{}', '{}', '{}'
            );"""

        ticker_id = 1
        for table_name,new_info in ultimate_data.items():    
            for k_date,v_price in new_info.items():
            
                pattern = r','
                date = datetime.strptime(re.sub(pattern,'',k_date), '%b %d %Y')
                price = float(re.sub(pattern,'',v_price))
                
                cursor.execute(insert_idx_table_values.format(table_name,date,price,ticker_id))
                
                ticker_id +=1
        conn_sql.commit()   
        
        cursor.close()
        conn_sql.close()









