# -*- coding: utf-8 -*-
"""
Created on Sat Sep  2 22:22:44 2017

@author: Calobeto
"""
import abc
import pandas as pd
import numpy as np

class ComputeProcess(object):
    
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def prepareDf(self):
        pass
        
    def neteomanager(self, params, data):
        """Sólo se procesa la data que debe procesarse. En caso ser paquetes se agrega la columna GROSS"""
        
        df = data.copy()
        
        # Si la columna GROSS no está creada en los paquetes. Se crea. 
        colsum = params['colsum']
        colfilter = params['colfilter']
        if colsum == '':
            grosslist = ['ADDS', 'NEWS', 'DEOF', 'REAC']
            #print(colsum)
            colsum = 'GROSS' # Calcular el GROSS.
            df[colsum] = df['ESTADO'].apply(lambda x : 1 if (x == grosslist[0] or x == grosslist[1] or x == grosslist[2] or x == grosslist[3]) else -1)
           
        # Ordenando la información
        df = df.sort(params['sortlist'], ascending = params['booleanlist'])
              
        # Agregar columnas adicionales : codigo_padre, neteo_telefono, neteo_codigo_padre, neteo_vendedor
        df['CODIGO_PADRE'] = df['CODIGO'].apply(lambda x: x[:len(x)-13] if x[0]!='1' else x)
        prefix = 'NETEO_'
     
        for col in params['colsecuence']:
            seriesum = df.groupby(col)[colsum].sum()
            dfsum = pd.DataFrame(seriesum).reset_index()
            dfsum.rename(columns = {colsum : prefix + col},inplace=True) 
            df = df.merge(dfsum, on = col)
            df = df[df[prefix + col] != 0]

        # El ultimo elemento de la lista colsecuence filtra aquellos vendedores que tengan solo deacs o paquetes 
        # siempre los positivos 
        df = df[df[prefix + col] > 0]
        
        # Ordenando nuevamente la información y agregando correlativo
        df = df.sort(params['sortlist'], ascending = params['booleanlist'])
        df.reset_index(inplace = True)
        df['CORRELATIVO'] = df.index.values

        # Aplicando el último filtro
        posvalues = df[[colfilter, 'CORRELATIVO']].groupby(colfilter).first()
        posvalues.rename(columns = {'CORRELATIVO' : 'ITEM'}, inplace = True)
        posvalues.reset_index(inplace = True)
        df = df.merge(posvalues, on = colfilter)
        
        # Si no hay FCHURN o DEOF puede lanzar un error
        df['STATE_FILTER'] = (df['CORRELATIVO'] - df['ITEM'] - df[prefix + colfilter].abs()).apply(lambda x : 1 if x < 0 else 0)
        df = df[df['STATE_FILTER'] > 0]           

        return df
        
class ComputeBolsas(ComputeProcess):
        
    def prepareDf(self, data):
        
        df = data.copy()
               
        df.drop_duplicates(['CONTRATO'], take_last=True, inplace=True)
        df['CANT_MIN_ANTER'].fillna(0, inplace=True)

        cantpercount = df.groupby('CUENTA')['CONTRATO'].count().reset_index()
        cantpercount.rename(columns = {'CONTRATO':'CANT_LINEAS3G'}, inplace=True)
        df = df.merge(cantpercount, on = 'CUENTA', how = 'left')
        df['ACCESSBOLSA'] = df['PRECIO']*(df['TOTAL_MIN']-df['CANT_MIN_ANTER']-df['CANT_MIN_DESACT'])/df['CANT_LINEAS3G']
        
        # Conviertiendo a cero los access 3g que son negativos ya que contratada es menor que la bolsa desactivada
        df.loc[df[df['ACCESSBOLSA']<0].index,'ACCESSBOLSA'] = 0
        
        df = df[df['ACCESSBOLSA'] != 0]
        df['ACCESSBOLSA'] = df['ACCESSBOLSA'].round(2)
        df = df[['CONTRATO', 'ACCESSBOLSA']]
        
        return df
        
class ComputeSumSSAA(ComputeProcess):
        
    def prepareDf(self, data):
        
        df = data.copy()
               
        df = df[['ACCESS_REAL', 'CONTRATO', 'ACTION_DATE']].groupby(['CONTRATO']).agg(['sum']).reset_index()

        # Aplanando las columnas
        a = df.columns.get_level_values(0)
        b = df.columns.get_level_values(1)
        c = [m + str(n) for m,n in zip(b,a)]
        df.columns = c
        df.rename # ver para que sirve
        #print(df.columns)
        df.rename(columns = {'sumACCESS_REAL' : 'ACCESSLICENCIA'}, inplace = True)
        df['ACCESSLICENCIA'] = df['ACCESSLICENCIA'].round(2)
            
        return df

class ComputePaquetes(ComputeProcess):
    
    def prepareDf(self, data):
        
        params = {'colsum' : '', 'colsecuence' : ['SERVICE', 'PHONENUMBER', 'CODIGO_PADRE', 'GANADOPORVOZ'], 
                  'colfilter' : 'GANADOPORVOZ', 'sortlist' : ['GANADOPORVOZ', 'GROSS', 'PHONENUMBER'], 
                  'booleanlist' : [True, False, True]}
        
        df = data.copy() # sólo para tener una copia de data antes del neteo
        
        # haciendo que ganadoporvoz y vendedor sea una sola columna
        nullganadoporvoz = df[df['GANADOPORVOZ'].isnull()].index
        df.loc[nullganadoporvoz, 'GANADOPORVOZ'] = df.loc[nullganadoporvoz,'VENDEDOR']
        
        # Aplicando Neteos y Filtrando sólo contratos que son gross. 
        df = self.neteomanager(params, df)
        #print(df.columns) # Punto de Control
        df['ACCESSPAQUETE'] = df['ACCESSPAQUETE'].round(2)
        
        return df
        
class ComputeGrossComision(ComputeProcess):
    
    def __init__(self, params, rules):
        self.rules = rules
        self.params = params
    
    def prepareDf(self, data): 
        
        df = data.copy()
        
        GROSS_CERO = 0

        cols = list(self.rules.columns.values)
        
        fullrowstochange = []
        for row in self.rules.itertuples():    
            # Removiendo el indice
            row = row[1:]
            match_list = list(row)
            #print(match_list) # Secuencia de Filtros
            realindex = [x for x in range(len(match_list)) if match_list[x] != '']
            criterios = [x for x in match_list if x != '']
            columns = [cols[x] for x in realindex ]
            rowstochange = df[df[columns].isin(criterios).all(axis = 1)].index.tolist()
            fullrowstochange = fullrowstochange + rowstochange
            
        df.loc[fullrowstochange, self.params['colchange']] = GROSS_CERO

        # Seleccionando sólo las filas que cambian para actualizar en bd
        df = df[df[self.params['colchange']] != df[self.params['colreference']]]
        df.reset_index(inplace = True)

        return df

class ComputeReversiones(ComputeProcess):
    
    def __init__(self, params, rules):
        self.rules = rules
        self.params = params    
    
    def prepareDf(self, data):
        
        df = data.copy()
              
        paramsdeac = {'colsum' : 'DEAC', 'colsecuence' : ['TELEFONO', 'CODIGO_PADRE', 'VENDEDOR_ACTIVACION'], 
                  'colfilter' : 'VENDEDOR_ACTIVACION', 'sortlist' : ['VENDEDOR_ACTIVACION', 'DEAC', 'TELEFONO'], 
                  'booleanlist' : [True, False, True]}
              
        # Aplicando Neteos y Filtrando sólo contratos que son deac. Tambien las posiciones que tienen reversiones
        df = self.neteomanager(paramsdeac, df)

        df = df[df['POSICION_EMPL'].notnull()] # logins en base de datos de lo contario se eliminan puestos

        
        positions = self.rules.drop_duplicates(['POSICION_EMPL'], take_last=True)['POSICION_EMPL']
        df = df[df['POSICION_EMPL'].isin(positions)]  
              
        df['ACCESS_TOTAL'] = df['ACCESS'] + df['ACCESSBOLSA'] + df['ACCESSPAQUETE'] + df['ACCESSLICENCIA']  
        df['FECHA_PROCESO'] = pd.to_datetime(df['FECHA_PROCESO'], dayfirst = True, coerce = True)
        df['FEC_ACTIV'] = pd.to_datetime(df['FEC_ACTIV'], dayfirst = True, coerce = True)     
        df['DIAS_DESACTIVADOS'] = (df['FECHA_PROCESO'] - df['FEC_ACTIV']).dt.days # dias calendario     
        df['RANGO_DESACTIVACION'] = df['DIAS_DESACTIVADOS'].apply(lambda x : 'Entre 0 y 90 dias' 
                                                                      if x < 91 else ('Entre 91 y 180 dias' 
                                                                                      if x < 181 else 'Mayor a 180 dias' ))
        #date1 = pd.Timestamp('2017-03-01')
        #date2 = pd.Timestamp('2017-09-01')
        #df['STATUS_APLICACION'] = df['FEC_ACTIV'].apply(lambda x : 'antes de Septiembre 2017' if x < date2 else 'despues de Septiembre 2017')        

        DEAC_DEFAULT = 0
        df[self.params['colchange']] = DEAC_DEFAULT
        
        cols = self.rules.columns.tolist()
        colsfactor = ['FACTOR_REVERSION','PESO_CAPTURA','TIPO_REVERSION']
        #i = 0        
        
        #reordering the cols
        #print(cols)
        colscriterios = [x for x in cols if x not in colsfactor]
        colsordered = colsfactor + colscriterios
        self.rules = self.rules[colsordered]       
        #df.to_csv('D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/'+ 'reversiones_brutas.csv')
        #self.rules.to_csv('D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/'+ 'reversiones_rules.csv')
        for row in self.rules.itertuples():    
            # Removiendo el indice y las columnas que tienen pesos o factores
            #i = i + 1
            rowfactor = row[1:len(colsfactor) + 1]
            factors = rowfactor[0] * rowfactor[1]
            rowcriterios = row[len(colsfactor) + 1:]               
            match_list = list(rowcriterios)
            realindex = [x for x in range(len(match_list)) if match_list[x] != '']
            criterios = [x for x in match_list if x != '']
            columns = [colscriterios[x] for x in realindex]
            rowstochange = df[df[columns].isin(criterios).all(axis = 1)].index.tolist()
            #print(rowfactor[2])
            if rowfactor[2] != 'NETEO':
                df.loc[rowstochange, self.params['colchange']] = - df.loc[rowstochange, rowfactor[2]] * factors
                df.loc[rowstochange, 'TIPO_REVERSION'] = 'REVERSION'
            else:
                #print(rowstochange)
                df.loc[rowstochange, self.params['colchange']] = 0
                df.loc[rowstochange, 'TIPO_REVERSION'] = 'NETEO'
                #print(df.loc[rowstochange, 'TIPO_REVERSION'])
                #df.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ 'reversiones_brutas' + str(i) + '.csv')
         
         # Ingresando el cálculo de penalidad
            #print(df.columns)
         
            rowspenal = df[df['PENALIDAD'] > 0].index.values
            df.loc[rowspenal, self.params['colchange']]= -df.loc[rowspenal,'COMISION_UNITARIA'] * (1-df.loc[rowspenal,'PENALIDAD'])
            
        #df.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ 'reversiones_brutas.csv')
        return df
        

            
