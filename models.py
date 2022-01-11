from sklearn.model_selection import train_test_split
from xgboost.sklearn import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV

from db_connector import DbConnector
import psycopg2

import json
import pandas as pd
import numpy as np
import joblib
import time

class Models:

    def __init__(self):
        self.params = {
            'max_depth' : [5,6,7,8,10,11],
            'learning_rate' : [0.005,0.01,0.025,0.05,0.1,0.15],
            'colsample_bynode' : [0.5,0.6,0.7,0.8,0.9,1.0], #max_features
            'colsample_bylevel' : [0.8,0.9,1.0],
            'colsample_bytree' : [0.6,0.7,0.8,0.9,1.0],
            'subsample' : [0.1,0.2,0.3,0.4,0.5,0.6,0.7],#max_samples
            'min_child_weight' : [5,10,15,20,25], #min_sample_leaf
            'reg_alpha' : [1,5,10,20,50],
            'reg_lambda' : [0.01,0.05,0.1,0.5,1],
            'n_estimators' : [400]
        }

        self.fit_params = {
            'eval_metric' : 'mae',
            'verbose' : False
        }
    
    def model_export(self, model):
        joblib.dump(model, 'outputs/best_model.pkl')

    def kpi_ML(self, Y_train, Y_train_pred, Y_test, Y_test_pred, name=''):
    
        df = pd.DataFrame(columns=['MAE','RMSE','Bias'],index=['Train', 'Test'])
        df.index.name=name
        
        df.loc['Train','MAE'] = 100*np.mean(abs(Y_train - Y_train_pred))/np.mean(Y_train)
        df.loc['Train','RMSE'] = 100*np.sqrt( np.mean((Y_train - Y_train_pred)**2) ) /np.mean(Y_train)
        df.loc['Train','Bias'] = 100*np.mean(Y_train - Y_train_pred)/np.mean(Y_train)
        df.loc['Test','MAE'] = 100*np.mean(abs(Y_test - Y_test_pred))/np.mean(Y_test)
        df.loc['Test','RMSE'] = 100*np.sqrt( np.mean((Y_test - Y_test_pred)**2) ) /np.mean(Y_test)
        df.loc['Test','Bias'] = 100*np.mean(Y_test - Y_test_pred)/np.mean(Y_test)
        df = df.astype(float).round(1)
        print(df) 

    def train_test_set(self, df,indexes_dict,exo_keys_list,x_len=36,test_size =0.15):
        """df: columnas contienen todos los tickers, las filas son las fechas
        """

        with open('outputs/x_len.json','w',encoding='utf-8') as f:
            json.dump(x_len, f)
        
        modeling_data = []
        for idx in indexes_dict:
            tmp_target = df[idx].values
            tmp_dict = {}
            for exo_idx in exo_keys_list:
                tmp_dict[exo_idx] = df[exo_idx].values

            periods = tmp_target.shape[0]
            #print(periods,type(periods))
            
            #dar formato a los datos
            y_len = 1
            loops = periods + 1 - x_len - y_len
            #print(loops)
            for loop in range(loops):
                tmp_data_list = []
                for exo_key,exo_value in tmp_dict.items():
                    tmp_data_list = np.append(tmp_data_list,tmp_dict[exo_key][loop:loop+x_len])

                tmp_data_list = np.append(tmp_data_list,tmp_target[loop:loop+x_len+y_len])
                modeling_data.append(tmp_data_list)
                
            X_md, Y_md = np.split(modeling_data,[-y_len],axis=1)
            X_train, X_test, Y_train, Y_test = train_test_split(X_md, Y_md, test_size=test_size)
                
            y_train = Y_train.ravel()
            y_test = Y_test.ravel()
        return X_train, X_test, y_train, y_test

    def training(self):

        db_connector = DbConnector()

        db_info = db_connector.db_info_data()

        conn_sql = psycopg2.connect( user = db_info['user'],
                           password = db_info['password'],
                           port = db_info['port'],
                           database = "finanzas_lite_db"
                           )
        cursor = conn_sql.cursor()

        with open('inputs/indexes.json','r',encoding='utf-8') as f:
            indexes = json.load(f)
        with open('inputs/exogenous_indexes.json','r',encoding='utf-8') as f:
                exo_indexes = json.load(f)
        idxs = indexes.copy()
        idxs.update(exo_indexes)

        consult_order = """ select * from {}_close_prices
        """

        my_data = {}
        for key,value in idxs.items():
           
            cursor.execute(consult_order.format(key))
            tmp_data_list = []
            for row in cursor:
                tmp_data_list.append(row)
           
            my_data[key] = tmp_data_list
        
        cursor.close()
        conn_sql.close()

        my_data_dfs = {}
        for key,value in idxs.items():
            my_data_dfs[key] = pd.DataFrame(my_data[key], columns=['id','date','close price', 'ticker id'] )
            my_data_dfs[key]['date'] = pd.to_datetime(my_data_dfs[key]['date'], format='%Y-%m-%d')
            my_data_dfs[key]['close price'] = pd.to_numeric(my_data_dfs[key]['close price'])
            my_data_dfs[key]['daily return'] = (my_data_dfs[key]['close price']/ my_data_dfs[key]['close price'].shift(1)) - 1
            my_data_dfs[key].loc[0,'daily return'] = 0
            my_data_dfs[key]['close price norm'] = ( my_data_dfs[key]['close price'] - my_data_dfs[key]['close price'].min()) / (my_data_dfs[key]['close price'].max() - my_data_dfs[key]['close price'].min())

        series = {}
        for idx,value in idxs.items():
            series[idx] = pd.Series(my_data_dfs[idx]['close price norm'].values, 
                                            index=my_data_dfs[idx]['date'].values,
                                        name = idx)
        
        values_list = [value for key,value in series.items()]
        data_series = pd.concat(values_list, axis=1)
        data_series.fillna(method ='bfill', inplace= True)
        data_series.fillna(method ='pad', inplace= True)

        exo_keys_list   = list(exo_indexes.keys())
        X_train, X_test, y_train, y_test = self.train_test_set(data_series,indexes,exo_keys_list )
        #print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)

        #TRAINING
        start = time.time()
        print('='*10)
        print('Proceso de Entrenamiento, puede tomar un tiempo considerable')
        print('='*10)
        XGB = XGBRegressor() #(n_jobs=1)
        XGB_cv = RandomizedSearchCV(XGB, self.params, cv=5, n_jobs=-1, verbose=1, n_iter=100, scoring='neg_mean_absolute_error')
        XGB_cv.fit(X_train,y_train,**self.fit_params)
        
        print(XGB_cv.best_params_)
        y_train_pred = XGB_cv.predict(X_train)
        y_test_pred = XGB_cv.predict(X_test)
        self.kpi_ML(y_train, y_train_pred.round(), y_test, y_test_pred.round(), name='XGBoost')
        stop = time.time()
        print(f'{round((stop-start)/60,3)} minutos')

        #OPTIMIZADOR DE ESTIMADORES
        print('Proceso de optimización de estimadores, puede tomar un tiempo considerable')
        params = XGB_cv.best_params_
        del params['n_estimators']
        n_estimators_list = [100,200,400,450,500,550,600,650,700,750,900,1000]
        rmse_train_list = []
        rmse_test_list = []
        mae_train_list = []
        mae_test_list = []
        for n  in n_estimators_list:
  
            XGB_bp = XGBRegressor(n_estimators=n, **XGB_cv.best_params_).fit(X_train,y_train)
       
            mae_value_test = np.mean(abs(y_test-XGB_bp.predict(X_test)))/np.mean(y_test)            
            mae_test_list.append(mae_value_test)

        estimators_min_error = min(range(len(mae_test_list)), key=mae_test_list.__getitem__)   
        print(f'Mejor Nº de estimadores: {n_estimators_list[estimators_min_error]}')

        official_estimators_num = n_estimators_list[estimators_min_error]

        #OPTIMIZACIÓN DE PARÁMETROS
        XGB_bp = XGBRegressor(n_estimators=official_estimators_num ,**XGB_cv.best_params_)
        XGB_bp.fit(X_train,y_train)

        y_train_pred = XGB_bp.predict(X_train)
        y_test_pred = XGB_bp.predict(X_test)
        self.kpi_ML(y_train, y_train_pred.round(), y_test, y_test_pred.round(), name='XGBoost')

        self.model_export(XGB_bp)







        

