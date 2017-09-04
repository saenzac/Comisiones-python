# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 11:20:19 2017

@author: calobeto
"""
import abc
import numpy as np
import pandas as pd
from pandas import Series, DataFrame
from datetime import datetime

class DataFramePreparation(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def prepareCols(self):
        pass
    
class HistoricalDataFrame(DataFramePreparation):
    
    def __init__(self, params):
        self.params = params
        
    def prepareCols(self):
        print('prepare historical data')
        
class PlainDataFrame(DataFramePreparation):
    
    # Dataframes que tienen columnas de fechas
    
    def __init__(self, params):
        self.params = params
        
    def prepareCols(self, data):      
        """ Convierte datetimes to string dates si usas sqlite """
        
        DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        ending = ['activacion' if self.params['section'] in ['tblEmpleados', 'tblVentaSSAA', 'tblPaquetes', 'tblVentas', 'tblJerarquia'] else 'desactivacion' if self.params['section'] in ['tblDeacs', 'tblDeacSSAA'] else 'migracion']
        keyperiod = 'periodo_' + ending[0]
        
        df = data.copy()
        df['fecha_actualizacion'] = datetime.now()
        
        #Agregando el periodo
        df[keyperiod] = df[self.params['colref']].map(lambda x: 100 * x.year + x.month)
        
        if self.params['section'] in ['tblVentas', 'tblDeacs']:
            df['fec_desactiva'] = pd.to_datetime(df['fec_desactiva'], dayfirst = True, coerce = True)
            if self.params['section'] == 'tblVentas':
                df['grosscomision'] = df['grosscomercial']
        
        # Convirtiendo datetime to string
        for col in self.params['coldates']:
            df[col] = df[col].astype('O')
            df[col] = df[col].apply(lambda x: x.strftime(DATETIME_FORMAT) if pd.notnull(x) else '')

        return keyperiod, df
    

    