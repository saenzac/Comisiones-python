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


class DataframeCleaner(object):
    
    def __init__(self, keycol, tipo, periodo):
        self.keycol = keycol
        self.tipo = tipo
        self.periodo = periodo
       
    def fillRows(self, subset, panel, keycol, col):
        """ 
        Permite detectar elementos de subset en panel y autollena. col es la columna adicional que se crea con el autollenado. 
        Si col = newkeycol se renombra newkeycol.
        """ 
        
        df = panel.copy()
        newkeycol = keycol
        if col == newkeycol:
            newkeycol = 'datos'
            df.rename(columns = {keycol:newkeycol}, inplace = True)

        df[col] = df[df[newkeycol].isin(subset[col].drop_duplicates().tolist())][newkeycol]
        df[col] = df[col].ffill()
        
        return df
    
    def rowsChange(self, subpanel, panel, subpanelcol, panelcol):      
        """ Busca el login equivalente en caso de logins no encontrados """
        
        df = panel.copy()

        # Detectando los datos a cambiar
        values_detected = df[df[panelcol].isin(subpanel[panelcol].drop_duplicates().tolist())]
        values_detected = values_detected.drop_duplicates(subset = [panelcol])
        for i in values_detected.index:
            r1 = values_detected.ix[i].values[0]
            r2 = subpanel.loc[subpanel[subpanel[panelcol] == r1].index, subpanelcol].values
            df.loc[df[panelcol] == r1, panelcol] = r2

        return df
        
        
    def kpiCleaner(self, kpis):

        # Asegúrate que item este codificado. 1XX para Kpis, 2XX para pesos, 3XX para Limitantes Mínimos y 4XX para 
        #Limitantes Máximos

        dft = kpis[kpis['item']!='item']
        dft = dft[~dft['item'].isnull()]

        #Filtro de sólo KPIs
        df = dft[dft['item']<2000]

        # Transformando la tabla
        columnnames = df.columns.tolist()[6:]
        dfi = pd.DataFrame()

        for column in columnnames:
            dfslice = df[['item','posición',column]]
            dftemp = dfslice.dropna()
            dftemp = dftemp.rename(columns={column:'metrica'})
            dftemp['tipo'] = column
            dfi = dfi.append(dftemp)

        df = dfi.reset_index(drop=True)

        return df  
            
    def detectValues(self, values_detected, data):
        
        df = values_detected.copy()
        df[self.periodo] = None

        # Ingresando resultados a los valores nulos. 
        for i in values_detected.index.values:
            iposicion = values_detected.ix[i].values[0]
            imetrica = values_detected.ix[i].values[1]
            itypeofkpi = values_detected.ix[i].values[2]
            iresultado = data.loc[(data[self.keycol] == iposicion) & (data['metrica'] == imetrica) & 
                                  (data['typeofkpi'] == itypeofkpi), self.periodo]
            if not(iresultado.empty):
                df.loc[i, self.periodo] = iresultado.values[0] # por defecto retorna series, el [0] es para que retorne un valor

        df.dropna(subset = [self.periodo], inplace = True)
        
        return df

    def setValues(self, col, values_detected, data):
        
        # reordenando columnas
        values_detected = values_detected[[col, 'metrica', 'typeofkpi', self.periodo]]
        
        qry = col + ' == @icol & metrica == @imetrica  & typeofkpi == @itypeofkpi'
        
        # Importante, afectando sólo a las personas que tienen valores nulos. Si se omite esto, afecta a las posiciones y
        # reemplaza a personas que hayan tenido buenas lecturas
        df = data.copy()
        df = df[df[self.periodo].isnull()]
        
        for i in values_detected.index.values:
            icol = values_detected.ix[i].values[0]
            imetrica = values_detected.ix[i].values[1]
            itypeofkpi = values_detected.ix[i].values[2]
            iresultado = values_detected.ix[i].values[3]
            indices = df.query(qry)
            data.loc[indices.index.values, self.periodo] = iresultado

        #print(qry) # ---> punto de control
        return data
    
    def sumValues(self, logins, kpis, data):
            
        df = data[data['metrica'].isin(kpis)]
        df = df[df[self.keycol].isin(logins)]
        df = df.groupby([self.keycol, 'typeofkpi'], as_index=False).sum()
        df['metrica'] = 'Gross Regiones'

        return df
    
    def getSupervisor(self, subpanel):
        
        #df = self.metricasconj.merge(subpanel, on = ['posición', 'metrica'], how = 'left')
        #print(self.metricasconj.columns)
        #print(subpanel.columns)
        df = self.metricasconj.merge(subpanel, on = ['posición', 'metrica','typeofkpi'], how = 'left')   
    
        # Reordenando columnas
        df = df[['nombres', 'metrica', 'typeofkpi', 'posición', 'kam']]
        return df
    
    def superMerge(self, subsetcol, panelcol, subset, panel, comisionantes):
        """ Las posiciones deben ser identicas para hacer el merge"""
        
        # Merging comisionantes y métricas
        dfc = comisionantes.merge(subset, on = subsetcol, how = 'left')
        #data_obj.to_csv(testpath + '_data_obj.csv')
        
        # se duplica registros en comisionantes para asignar objetivos y resultados
        dfr = dfc.copy()
        dfc['typeofkpi'] = 'objetivos'
        dfr['typeofkpi'] = 'resultados'
        df = dfc.append(dfr)
        df = df.reset_index(drop=True)

        # Merging comisionantes y panel 
        df = df.merge(panel, on = [self.keycol, panelcol,'typeofkpi'], how = 'left')
        df.dropna(subset = ['tipo'], inplace = True)
        
        # Completando el panel ya que hay kpis que son cuotas por posición. Se completa el typeofkpi.
        #data.loc[data['typeofkpi'].isnull(),'typeofkpi'] = 'objetivos'
        
        return df
        
    def wrangler(self, data = None, results = None):
        """ data es objetivos o panel de plataformas """
        
        replvalues1 = 'Objetivo|objetivo|Cuota|cuota'
        replvalues2 = 'Obj|obj|Obj.'
        replvalues3 = '(- .)$|(- )$|\(\)$|\(\.\)$'
        replvalues4 = 'Venta TPF' # esta secuencia es importante para detectar TPF antes de TP
        replvalues5 = 'Venta TP|Venta TC|Venta Islas'
        
        objvalues = 'Obj|obj|Cuota|cuota'
        #navalues = 'DIV/0!.*|.*REF.*|,|-|2da|2d%|.'
        new_kpi_superv = 'Ventas Totales'
        
        if self.tipo == 'voz':
            objetivos = data.copy()
            resultados = results.copy()
            objetivos.rename(columns = {'datos':self.keycol}, inplace = True)
            resultados.rename(columns = {'datos':self.keycol}, inplace = True)
            objetivos['typeofkpi'] = 'objetivos'
            resultados['typeofkpi'] = 'resultados'
            df = objetivos.append(resultados)
        
        elif self.tipo == 'plataformas':           
            df = data.copy()
            df.rename(columns = {'datos':self.keycol}, inplace = True)
            df['typeofkpi'] = 'resultados'
            #df[self.periodo].replace(navalues, np.NaN, regex=True, inplace=True)
            df.loc[df[self.keycol].str.contains(objvalues, na=False),'typeofkpi'] = 'objetivos'
            df[self.keycol] = df[self.keycol].str.replace(replvalues1, "")
            df[self.keycol] = df[self.keycol].str.replace(replvalues2, "")
            df[self.keycol] = df[self.keycol].str.replace(replvalues3, "")
            df[self.keycol] = df[self.keycol].str.replace(replvalues4, new_kpi_superv)
            df[self.keycol] = df[self.keycol].str.replace(replvalues5, new_kpi_superv)
            df[self.keycol] = df[self.keycol].str.rstrip()
            
        df.reset_index(drop = True, inplace = True)       
        
        return df  
    
    def powerPivot(self, panel):
        """
        http://stackoverflow.com/questions/39229005/pivot-table-no-numeric-types-to-aggregate
        No poner KAM en el pivot ya que sale cortado
        
        Por defecto aggfunc es mean pero hay valores nulos (como los nuevos ingresos), por ello se escoge
        aggfunc='first'
        """

        caja_dic=[{'caja':'venta','nivel_caja':1},{'caja':'gestión','nivel_caja':2},{'caja':'desarrollo','nivel_caja':3}]
        
        df = panel.copy()
        df['caja'] = df['tipo'].apply(lambda x: x[:len(x)-2])

        df_nivel_caja = pd.DataFrame(caja_dic)

        df = df.merge(df_nivel_caja, on = 'caja', how = 'left')
        
        pivot = df.pivot_table(index = ['item', 'gerencia2','zona', 'posición',self.keycol], 
                               columns=['typeofkpi','nivel_caja','tipo'], values = self.periodo, aggfunc='first')
        
        return pivot

class DataFramePreparation(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def prepareCols(self):
        pass

class HistoricalDataFrame(DataFramePreparation):
    
    def __init__(self, params):
        self.params = params
        
        
    def prepareCols(self):
        testpath = 'D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/'
        tipo = self.params['tipo']
        periodo = self.params['periodo']
        keycol = self.params['keycol']
        loginseq = self.params['logins']
        kpis = self.params['kpis']
        comisionantes = self.params['comisionantes']
        cuotas = self.params['cuotas']
        resultados = self.params['resultados']
        periodo = self.params['periodo']
        metricasconjuntas = self.params['metricasconjuntas']
        
        powercleaner = DataframeCleaner(keycol, tipo, periodo)
        
        loginseq.rename(columns = {'login_equivalente' : keycol}, inplace = True)      
        kpis = powercleaner.kpiCleaner(kpis)
        panel = powercleaner.wrangler(cuotas, resultados)
        #print(kpis.columns)
        #kpist.to_csv(testpath + '_kpis.csv') #---> Punto de Control
        #panel.to_csv(testpath + '_panel.csv') #---> Punto de Control
        if self.params['tipo'] == 'voz':        
            panel = powercleaner.fillRows(kpis, panel, keycol, 'metrica')
            panel = powercleaner.rowsChange(loginseq, panel, 'login_real', keycol)
            panel.dropna(subset = [periodo], inplace = True)
            panel.drop_duplicates(subset = [keycol, 'metrica','typeofkpi'], inplace = True)
             #---> Punto de Control
            planilla = powercleaner.superMerge('posición', 'metrica', kpis, panel, comisionantes)
            nullpositions = planilla[planilla[periodo].isnull()][['posición','metrica','typeofkpi']].drop_duplicates().reset_index(drop=True)
            #metrics_conjunt = powercleaner.getSupervisor(nullpositions)            
            valuesposition = powercleaner.detectValues(nullpositions, panel)
            planilla = powercleaner.setValues('posición', valuesposition, planilla)
            df = powercleaner.powerPivot(planilla)
            
        elif self.params['tipo'] == 'plataformas':
            panel = powercleaner.fillRows(kpis, panel, keycol, 'metrica')
            panel = powercleaner.rowsChange(loginseq, panel, 'login_real', keycol)
            panel = powercleaner.fillRows(comisionantes, panel, keycol, keycol)       
            #panel.to_csv(testpath + month + '_panelpltf.csv') #---> Punto de Control
            panel = panel[panel['datos'] == panel['metrica']]
            panel.drop_duplicates(subset = [keycol, 'metrica', 'typeofkpi'], inplace = True)
            planilla = powercleaner.superMerge('posición', 'metrica', kpis, panel, comisionantes)
            nullpositions = planilla[planilla[periodo].isnull()][['kam', 'posición', 'metrica', 'typeofkpi']].drop_duplicates().reset_index(drop=True)
            #metricsconjunt = powercleaner.getSupervisor(nullpositions)
            
            # Detectando el Supervisor
            metricsconjunt = metricasconjuntas.merge(nullpositions, on = ['posición', 'metrica','typeofkpi'], how = 'left')   
            # Reordenando columnas)        
            metricsconjunt = metricsconjunt[['nombres', 'metrica', 'typeofkpi', 'posición', 'kam']]
            valuesposition = powercleaner.detectValues(metricsconjunt, panel)
            planilla = powercleaner.setValues('posición', valuesposition, planilla)
            df = powercleaner.powerPivot(planilla)
            
        #nullpositions.to_csv(testpath + month + '_posiciones_nulas.csv')     # ---> punto de control
        #valuesposition.to_csv(testpath + month + '_metricas_supervisor.csv') # ---> punto de control
        
        # Dejando Logins equivalentes como antes
        loginseq.rename(columns = {keycol : 'login_equivalente'}, inplace = True)
           
        return df
    
        
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

    