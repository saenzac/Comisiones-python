# -*- coding: utf-8 -*-
"""
Created on Sat Sep  2 22:22:44 2017
a

@author: Calobeto
"""
import abc
import pandas as pd
import numpy as np

import logging

# Initializing the logger instance
logger = logging.getLogger("juplogger")

class ComputeProcess(object):
    
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def prepareDf(self):
        pass

    """
    xx
    data : dataframe of DB temporal table of Reversiones
    params: Nombres de columna del dataframe que 
    params = {'colsum'       : 'DEAC',
              'colsecuence'  : ['TELEFONO', 'CODIGO_PADRE', 'VENDEDOR_ACTIVACION'],
              'colfilter'    : 'VENDEDOR_ACTIVACION',
              'sortlist'     : ['VENDEDOR_ACTIVACION', 'DEAC', 'TELEFONO'],
              'booleanlist'  : [True, False, True]}
    """
    def neteomanager(self, params, data):
        #Sólo se procesa la data que debe procesarse. En caso ser paquetes se agrega la columna GROSS
        df = data.copy()

        # Si la columna GROSS no está creada en los paquetes. Se crea.
        colsum = params['colsum']
        colfilter = params['colfilter']
        if colsum == '':
            grosslist = ['ADDS', 'NEWS', 'DEOF', 'REAC']
            colsum = 'GROSS' # Calcular el GROSS.
            df[colsum] = df['ESTADO'].apply(lambda x : 1 if (x == grosslist[0] or x == grosslist[1] or x == grosslist[2] or x == grosslist[3]) else -1)

        # Ordenando la información
        df = df.sort_values(by = params['sortlist'], ascending = params['booleanlist'])

        # Agregar columnas adicionales : codigo_padre, neteo_telefono, neteo_codigo_padre, neteo_vendedor
        df['CODIGO_PADRE'] = df['CODIGO'].apply(lambda x: x[:len(x)-13] if x[0]!='1' else x)
        prefix = 'NETEO_'

        for col in params['colsecuence']:
            # Agrupamos con columna  indice "col" y columna de agrupación colsum.
            # Notar que sin especificar [colsum] la expresión df.groupby(col) retorna un objeto de tipo DataFrameGroupByes
            # el cual al aplicarle sum() devuelve un dataframe con todas sus columnas agrupadas.
            # Al especificar [colsum]  se devuelve un SeriesGroupBy y por lo tanto 'seriesum' es una serie de 2 columnas: col(no agrupada) y colsum(agrupada).
            seriesgroup = df.groupby(col)[colsum]
            seriesum = seriesgroup.sum()
            #Se crea un dataframe a partir de la serie anterior y se resetea el indice
            dfsum = pd.DataFrame(seriesum).reset_index()
            #Se cambia el nombre de 'colsum' a uno que identifica la
            dfsum.rename(columns = {colsum : prefix + col},inplace=True)
            #El dataframe original es fusionado con el nuevo dataframe reducido: dfsum
            df = df.merge(dfsum, on = col)
            #Solo mantiene los registros diferentes de cero en la columna 'prefix + col'
            df = df[df[prefix + col] != 0]

        # El ultimo elemento de la lista colsecuence filtra aquellos vendedores que tengan solo deacs o paquetes 
        # siempre los positivos 
        df = df[df[prefix + col] > 0]
        
        # Ordenando nuevamente la información y agregando correlativo
        df = df.sort_values(by=params['sortlist'], ascending = params['booleanlist'])
        df.reset_index(inplace = True)
        df['CORRELATIVO'] = df.index.values # numeracion del 1 hacia arriba la guardamos en la columna CORRELATIVO

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
               
        df.drop_duplicates(['CONTRATO'], keep='last', inplace=True)
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
               
        df = df[['ACCESSREAL', 'CONTRATO', 'ACTION_DATE']].groupby(['CONTRATO']).agg(['sum']).reset_index()

        # Aplanando las columnas
        a = df.columns.get_level_values(0)
        b = df.columns.get_level_values(1)
        c = [m + str(n) for m,n in zip(b,a)]
        df.columns = c
        #df.rename # ver para que sirve
        #print(df.columns) # Test
        df.rename(columns = {'sumACCESSREAL' : 'ACCESSLICENCIA'}, inplace = True)
        df['ACCESSLICENCIA'] = df['ACCESSLICENCIA'].round(2)
            
        return df

class ComputePaquetes(ComputeProcess):

    def prepareDf(self, data):

        params = {'colsum' : '', 'colsecuence' : ['SERVICE', 'PHONENUMBER', 'CODIGO_PADRE', 'GANADOPORVOZ'], 
                  'colfilter' : 'GANADOPORVOZ', 'sortlist' : ['GANADOPORVOZ', 'GROSS', 'PHONENUMBER'], 
                  'booleanlist' : [True, False, True]}

        df = data.copy() # sólo para tener una copia de data antes del neteo

        # haciendo que ganadoporvoz y vendedor sea una sola columna
        ## nullganadoporvoz = df[df['GANADOPORVOZ'].isnull()].index
        ##df.loc[nullganadoporvoz, 'GANADOPORVOZ'] = df.loc[nullganadoporvoz,'VENDEDOR']
        
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

"""
    Description for class
        Clase que se encarga de calcular las reversiones de Voz.
        No calcula reversiones VAS.
"""
class ComputeReversiones(ComputeProcess):
    #Inicializador de la clase
    def __init__(self, params, rules):
        # Dataframe de la tabla tblsopreversiones_rules
        self.rules = rules
        # Diccionario de parametros de la clase DbDataProcess.
        # Contiene parametros leidos de archivo *.ini y creados en la ejecución.
        self.params = params    

    #Ejecuta el calculo de las reversiones. El resultado se da en el dataframe "df"  salida de la función.
    def prepareDf(self, data):

        #data: dataframe de la tabla temporal  "reversiones_temp" de la BD, la cual fue creada a partir de la tabla
        #      principal "reversiones". Creada en datahandledatabase->DbDataProcess.pre_loadData()
        df = data.copy()

        paramsdeac = {'colsum'       : 'DEAC',
                      'colsecuence'  : ['TELEFONO', 'CODIGO_PADRE', 'VENDEDOR_ACTIVACION'],
                      'colfilter'    : 'VENDEDOR_ACTIVACION',
                      'sortlist'     : ['VENDEDOR_ACTIVACION', 'DEAC', 'TELEFONO'],
                      'booleanlist'  : [True, False, True]}

        # Aplicando Neteos y Filtrando sólo contratos que son deac. Tambien las posiciones que tienen reversiones
        df = self.neteomanager(paramsdeac, df)

        # Se descartan "contratos" vendidos por personas que no figuran en la tabla de empleados.
        df = df[df['POSICION_EMPL'].notnull()]

        #Eliminamos posiciones repetidas de la tabla reversiones rules de db
        positions = self.rules.drop_duplicates(['POSICION_EMPL'], keep='last')['POSICION_EMPL']
        
        #Del view reversiones 
        df = df[df['POSICION_EMPL'].isin(positions)]  

        df['FECHA_PROCESO'] = pd.to_datetime(df['FECHA_PROCESO'], dayfirst = True, errors='coerce')
        df['FEC_ACTIV'] = pd.to_datetime(df['FEC_ACTIV'], dayfirst = True, errors='coerce')
        df['DIAS_DESACTIVADOS'] = (df['FECHA_PROCESO'] - df['FEC_ACTIV']).dt.days # dias calendario
        
        df['RANGO_DESACTIVACION'] = df['DIAS_DESACTIVADOS'].apply(lambda x : 'Entre 0 y 240 dias'
                                                                      if x < 241 else ('Entre 241 y 360 dias' 
                                                                                      if x < 361 else 'Mayor a 360 dias' ))    

        DEAC_DEFAULT = 0
        df[self.params['colchange']] = DEAC_DEFAULT

        cols = self.rules.columns.tolist()
        colsfactor = ['FACTOR_REVERSION','PESO_CAPTURA','TIPO_REVERSION']
        #i = 0        

        #reordering the cols
        #print(cols)
        #colsordered: ['FACTOR_REVERSION', 'PESO_CAPTURA', 'TIPO_REVERSION', 'POSICION_EMPL', 'RANGO_DESACTIVACION', 'PACK_CHIP', 'PORTABILIDAD', 'CATEGORIA_MOTIVO_DEAC', 'CATEGORIA_TECNOLOGIAEQUIPO']
        colscriterios = [x for x in cols if x not in colsfactor]
        colsordered = colsfactor + colscriterios
        self.rules = self.rules[colsordered]
        ### Trabajar este Archivo reversiones brutas. En caso que aparezca FChurn, DEOF, ADDOF o NEWOF netearlo con algun deac de la cuenta o consultor
        ### Calcular la reversión unitaria en excel tal como se indica en 
        ### Tener en cuenta la columna penalidad. Si 0 se revierte, si tiene 100% de penalidad no le corresponde reversion, Si tiene 25% de penalidad sobre aplicarle
        ### el 75% de la reversión

        #df.to_csv('D:/Datos de Usuario/jsaenza/Documents/OneDrive - Entel Peru S.A/MercadoEmpresas/Data Fuente Comisiones/test/'+ 'reversiones_brutas.csv')
        #self.rules.to_csv('D:/Datos de Usuario/jsaenza/Documents/OneDrive - Entel Peru S.A/MercadoEmpresas/Data Fuente Comisiones/test/'+ 'reversiones_rules.csv')
        logging.debug("Aca escribia los archivos de reversiones brutas y reversiones rules")
        for row in self.rules.itertuples():
            # Removiendo el indice y las columnas que tienen pesos o factores
            rowfactor = row[1:len(colsfactor) + 1]
            factors = rowfactor[0] * rowfactor[1]
            #len(colsfactor) = 3
            #row es una tupla, al hacer un slice sigue siendo una tupla.
            rowcriterios = row[len(colsfactor) + 1:]
            #convierte la tupla rowcriterios a una lista
            match_list = list(rowcriterios)
            realindex = [x for x in range(len(match_list)) if match_list[x] != '']
            criterios = [x for x in match_list if x != '']
            columns = [colscriterios[x] for x in realindex]
            rowstochange = df[df[columns].isin(criterios).all(axis = 1)].index.tolist()

            tipo_de_reversion = rowfactor[2]
            if tipo_de_reversion == 'COMISION_UNITARIA':
                df.loc[rowstochange, self.params['colchange']] = - df.loc[rowstochange, tipo_de_reversion] * df.loc[rowstochange, "ACCESS_EJECUTIVO"] * factors
                df.loc[rowstochange, 'TIPO_REVERSION'] = 'REVERSION COMISION UNITARIA'
            elif tipo_de_reversion == 'ACCESS':
                factor_mesa_precios = df.loc[rowstochange, "FACTOR_MESA_PRECIOS"]
                df.loc[rowstochange, self.params['colchange']] = - df.loc[rowstochange, tipo_de_reversion] * factors * factor_mesa_precios
                df.loc[rowstochange, 'TIPO_REVERSION'] = 'REVERSION ACCESS PURO'
            else:
                raise Exception("Tipo de reversion invalido, revisar tabla DB tblsopreversiones_rules")
                sys.exit(1)
            
        #No revertimos los contratos cuyo vendedor en el mes de activacion no comisiono.
        indices_contratos_no_comisionaron = df[df['COMISIÓN'] == 0].index.values
        df.loc[indices_contratos_no_comisionaron, self.params['colchange']] = 0
        df.loc[indices_contratos_no_comisionaron, 'TIPO_REVERSION'] = 'No comisiono'

        #No revertimos los contratos que se pagaron con factor de mesa de precios por duracion menor a 18 meses.
        indices_contratos_factor18m = df[df['TIENE_FACTOR_MESA_P_MENOR18M'] == 1].index.values
        df.loc[indices_contratos_factor18m, self.params['colchange']] = 0
        df.loc[indices_contratos_factor18m, 'TIPO_REVERSION'] = 'Factor MP < 18M'
            #else:
                #print(rowstochange)
             #   df.loc[rowstochange, self.params['colchange']] = 0
             #   df.loc[rowstochange, 'TIPO_REVERSION'] = 'NETEO'
                #print(df.loc[rowstochange, 'TIPO_REVERSION']) # Punto de Control
                #df.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ 'reversiones_brutas' + str(i) + '.csv')

         # Ingresando el cálculo de penalidad
            #print(df.columns)
         
            ##rowspenal = df[df['PENALIDAD'] > 0].index.values
            ##df.loc[rowspenal, self.params['colchange']]= -df.loc[rowspenal,'COMISION_UNITARIA'] * (1-df.loc[rowspenal,'PENALIDAD'])
            
        #df.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ 'reversiones_brutas.csv')
        return df
        

            
