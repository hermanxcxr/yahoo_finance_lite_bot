from db_connector import DbConnector
import psycopg2

import json
import pandas as pd
import numpy as np
import joblib
import time

class RegularExecution:

    def __init__(self):
        self.model = joblib.load('outputs/best_model.pkl')

        with open('inputs/indexes.json','r',encoding='utf-8') as f:
            self.indexes = json.load(f)
        with open('inputs/exogenous_indexes.json','r',encoding='utf-8') as f:
            self.exo_indexes = json.load(f)
        self.idxs = self.indexes.copy()
        self.idxs.update(self.exo_indexes) 

        with open('outputs/x_len.json','r',encoding='utf-8') as f: 
            self.x_len = json.load(f)


    
    def forecast(self):
        db_connector = DbConnector()

        db_info = db_connector.db_info_data()

        conn_sql = psycopg2.connect( user = db_info['user'],
                           password = db_info['password'],
                           port = db_info['port'],
                           database = "finanzas_lite_db"
                           )
        cursor = conn_sql.cursor()

        last_x_len_data = """select date, close_price 
                            from {}_close_prices 
                            ORDER BY spc_id DESC LIMIT {};"""
        
        my_vectors = {}
        for idx,ticker in self.idxs.items():
            cursor.execute(last_x_len_data.format(idx,self.x_len))
            conn_sql.commit()
            
            tmp_date = []
            tmp_price = []
            tmp_dict = {}
            for row in cursor:
                tmp_date.append(row[0])
                tmp_price.append(row[1])
            tmp_dict['date'] = tmp_date
            tmp_dict['price'] = tmp_price
            
            my_vectors[idx] = tmp_dict
        
        cursor.close()
        conn_sql.close()

        series = {}
        for idx,value in self.idxs.items():
            series[idx] = pd.Series(my_vectors[idx]['price'],
                                    index=my_vectors[idx]['date'],
                                    name = idx)
        
        data_series = pd.concat(list(series.values()), axis=1)
        data_series.fillna(method ='bfill', inplace= True)
        data_series.fillna(method ='pad', inplace= True)
        data_series = data_series.iloc[:self.x_len,:]

        definitive_vectors = {}
        for idx in self.indexes:
            tmp_target = np.flip(pd.to_numeric(data_series[idx].values))    
            tmp_target = (tmp_target - tmp_target.min()) / (tmp_target.max() - tmp_target.min())
            tmp_dict = {}
            for exo_idx in self.exo_indexes:
                tmp_value = np.flip(pd.to_numeric(data_series[exo_idx].values))
                tmp_dict[exo_idx] = (tmp_value - tmp_value.min()) / (tmp_value.max() - tmp_value.min())
            
            tmp_data_list = []
            for exo_key,exo_value in tmp_dict.items():
                tmp_data_list = np.append(tmp_data_list,tmp_dict[exo_key])
            
            tmp_data_list = np.append(tmp_data_list,tmp_target)
            definitive_vectors[idx] = tmp_data_list

        predictions = {}
        for idx in self.indexes:
            predictions[idx] = self.model.predict(definitive_vectors[idx].reshape(1,-1))

        comparatives = {}
        for idx in self.indexes:
            if predictions[idx] >  definitive_vectors[idx][-1]:
                comparatives[idx] = 'subirá'
            elif predictions[idx] <  definitive_vectors[idx][-1]:
                comparatives[idx] = 'bajará'
            else:
                comparatives[idx] = 'se mantendrá estable'    

        for idx in self.indexes.keys():
            print('El módelo predice que el precio para {} {} hoy'.format(idx ,comparatives[idx]))
        

    
