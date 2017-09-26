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
            colsum = 'gross' # Calcular el GROSS.
            df[colsum] = df['estado'].apply(lambda x : 1 if (x == grosslist[0] or x == grosslist[1] or 
                                                             x == grosslist[2] or x == grosslist[3]) else -1)
        # Ordenando la información
        df = df.sort(params['sortlist'], ascending = params['booleanlist'])
              
        # Agregar columnas adicionales : codigo_padre, neteo_telefono, neteo_codigo_padre, neteo_vendedor
        df['codigo_padre'] = df['codigo'].apply(lambda x: x[:len(x)-13] if x[0]!='1' else x)
        prefix = 'neteo_'
       
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
        df['correlativo'] = df.index.values
        
        # Aplicando el último filtro
        posvalues = df[[colfilter, 'correlativo']].groupby(colfilter).first()
        posvalues.rename(columns = {'correlativo' : 'item'}, inplace = True)
        posvalues.reset_index(inplace = True)
        df = df.merge(posvalues, on = colfilter)
        
        # Si no hay FCHURN o DEOF puede lanzar un error
        df['state_filter'] = (df['correlativo'] - df['item'] - df[prefix + colfilter].abs()).apply(lambda x : 1 if x < 0 else 0)
        df = df[df['state_filter'] > 0]           

        return df
        
class ComputeBolsas(ComputeProcess):
        
    def prepareDf(self, data):
        
        df = data.copy()
               
        df.drop_duplicates(['contrato'], take_last=True, inplace=True)
        df['cant_min_anter'].fillna(0, inplace=True)

        cantpercount = df.groupby('cuenta')['contrato'].count().reset_index()
        cantpercount.rename(columns = {'contrato':'cant_lineas3g'}, inplace=True)
        df = df.merge(cantpercount, on = 'cuenta', how = 'left')
        df['accessbolsa'] = df['precio']*(df['total_min']-df['cant_min_anter']-df['cant_min_desact'])/df['cant_lineas3g']
        
        # Conviertiendo a cero los access 3g que son negativos ya que contratada es menor que la bolsa desactivada
        df.loc[df[df['accessbolsa']<0].index,'accessbolsa'] = 0
        
        df = df[df['accessbolsa'] != 0]
        df['accessbolsa'] = df['accessbolsa'].round(2)
        df = df[['contrato', 'accessbolsa']]
        
        return df
        
class ComputeSumSSAA(ComputeProcess):
        
    def prepareDf(self, data):
        
        df = data.copy()
               
        df = df[['access', 'contrato', 'action_date']].groupby(['contrato']).agg(['sum', 'count']).reset_index()

        # Aplanando las columnas
        a = df.columns.get_level_values(0)
        b = df.columns.get_level_values(1)
        c = [m + str(n) for m,n in zip(b,a)]
        df.columns = c
        df.rename # ver para que sirve
        df.rename(columns = {'sumaccess' : 'accesslicencia', 'countaccess' : 'countlicencia'}, inplace = True)
        df['accesslicencia'] = df['accesslicencia'].round(2)
            
        return df

class ComputePaquetes(ComputeProcess):
    
    def prepareDf(self, data):
        
        params = {'colsum' : '', 'colsecuence' : ['service', 'phonenumber', 'codigo_padre', 'ganadoporvoz'], 
                  'colfilter' : 'ganadoporvoz', 'sortlist' : ['ganadoporvoz', 'gross', 'phonenumber'], 
                  'booleanlist' : [True, False, True]}
        
        df = data.copy() # sólo para tener una copia de data antes del neteo
        
        # haciendo que ganadoporvoz y vendedor sea una sola columna
        nullganadoporvoz = df[df['ganadoporvoz'].isnull()].index
        df.loc[nullganadoporvoz, 'ganadoporvoz'] = df.loc[nullganadoporvoz,'vendedor']
        
        # Aplicando Neteos y Filtrando sólo contratos que son gross. 
        df = self.neteomanager(params, df)
        df['accesspaquete'] = df['accesspaquete'].round(2)
        
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
              
        paramsdeac = {'colsum' : 'deac', 'colsecuence' : ['telefono', 'codigo_padre', 'vendedor_activacion'], 
                  'colfilter' : 'vendedor_activacion', 'sortlist' : ['vendedor_activacion', 'deac', 'telefono'], 
                  'booleanlist' : [True, False, True]}
        
        
        # Aplicando Neteos y Filtrando sólo contratos que son deac. Tambien las posiciones que tienen reversiones
        df = self.neteomanager(paramsdeac, df)
        df = df[df['posicion_empl'].notnull()] # logins en base de datos de lo contario se eliminan puestos

        
        positions = self.rules.drop_duplicates(['posicion_empl'], take_last=True)['posicion_empl']
        df = df[df['posicion_empl'].isin(positions)]  
              
        df['access_total'] = df['access'] + df['accessbolsa'] + df['accesspaquete'] + df['accesslicencia']  
        df['fecha_proceso'] = pd.to_datetime(df['fecha_proceso'], dayfirst = True, coerce = True)
        df['fec_activ'] = pd.to_datetime(df['fec_activ'], dayfirst = True, coerce = True)     
        df['dias_desactivados'] = (df['fecha_proceso'] - df['fec_activ']).dt.days # dias calendario     
        df['rango_desactivacion'] = df['dias_desactivados'].apply(lambda x : 'Entre 0 y 90 dias' 
                                                                      if x < 91 else ('Entre 91 y 180 dias' 
                                                                                      if x < 181 else 'Mayor a 180 dias' ))
        df['status_aplicacion'] = 'despues de Marzo 2017'
        date_before = pd.Timestamp('2017-03-01')
        df['status_aplicacion'] = df['fec_activ'].apply(lambda x : 'antes de Marzo 2017' if x < date_before 
                                                        else 'despues de Marzo 2017')
        
        DEAC_DEFAULT = 0
        df[self.params['colchange']] = DEAC_DEFAULT
        
        cols = self.rules.columns.tolist()
        colsfactor = ['factor_reversion','peso_captura','tipo_reversion']
        
        #reordering the cols

        colscriterios = [x for x in cols if x not in colsfactor]
        colsordered = colsfactor + colscriterios
        self.rules = self.rules[colsordered]       

        for row in self.rules.itertuples():    
            # Removiendo el indice y las columnas que tienen pesos o factores
            rowfactor = row[1:len(colsfactor) + 1]
            factors = rowfactor[0] * rowfactor[1]
            rowcriterios = row[len(colsfactor) + 1:]               
            match_list = list(rowcriterios)
            realindex = [x for x in range(len(match_list)) if match_list[x] != '']
            criterios = [x for x in match_list if x != '']
            columns = [colscriterios[x] for x in realindex]
            rowstochange = df[df[columns].isin(criterios).all(axis = 1)].index.tolist()
            if rowfactor[2] != 'neteo':
                df.loc[rowstochange, self.params['colchange']] = - df.loc[rowstochange, rowfactor[2]] * factors
                df.loc[rowstochange, 'tipo_descuento'] = 'reversión'
            else:
                df.loc[rowstochange, self.params['colchange']] = 0
                df.loc[rowstochange, 'tipo_descuento'] = 'neteo'
         
         # Ingresando el cálculo de penalidad
         
            rowspenal = df[df['penalidad'] > 0].index.values
            df.loc[rowspenal, self.params['colchange']]= -df.loc[rowspenal,'comision_unitaria'] * (1-df.loc[rowspenal,'penalidad'])
            
        return df
        

            
