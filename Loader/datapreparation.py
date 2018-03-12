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
            newkeycol = 'DATOS'
            df.rename(columns = {keycol:newkeycol}, inplace = True)
        
        df[col] = df[df[newkeycol].isin(subset[col].drop_duplicates().tolist())][newkeycol]
        df[col] = df[col].ffill()
        
        return df
    
    def rowsChange(self, logins, panel, loginscol, panelcol):      
        """ Busca el login equivalente en caso de logins no encontrados """
        
        #df = panel.dropna()
        df = panel.copy()

        # Detectando los datos a cambiar
        values_detected = df[df[panelcol].isin(logins[panelcol].drop_duplicates().tolist())]
        values_detected = values_detected.drop_duplicates(subset = [panelcol])
        for i in values_detected.index:
            r1 = values_detected.ix[i].values[0]
            r2 = logins.loc[logins[logins[panelcol] == r1].index, loginscol].values
            #print(r2) # Punto de Control
            df.loc[df[panelcol] == r1, panelcol] = r2

        return df
        
        
    def kpiCleaner(self, kpis):

        # Asegúrate que item este codificado. 1XXX para Kpis, 2XXX para pesos, 3XXX para Limitantes Mínimos y 4XXX para limitantes Máximos

        dft = kpis[kpis['ITEM']!='ITEM']
        dft = dft[~dft['ITEM'].isnull()]

        #Filtro de sólo KPIs
        df = dft[dft['ITEM']<2000]

        # Pesos por kpi
        #kpi_pesos = dft[dft['ITEM']>1999 and dft['ITEM']<3000]

        # Transformando la tabla
        columnnames = df.columns.tolist()[6:]
        dfi = pd.DataFrame()

        for column in columnnames:
            dfslice = df[['ITEM','POSICIÓN',column]]
            dftemp = dfslice.dropna()
            dftemp = dftemp.rename(columns={column:'METRICA'})
            dftemp['TIPO'] = column
            dfi = dfi.append(dftemp)

        df = dfi.reset_index(drop=True)
   
        return df  
            
    def detectValues(self, values_detected, data):
        
        df = values_detected.copy()
        df[self.periodo] = None
        
        # Ingresando resultados a los valores nulos. 
        for i in values_detected.index.values:
            ifiltro = values_detected.ix[i].values[0]
            imetrica = values_detected.ix[i].values[2]
            itypeofkpi = values_detected.ix[i].values[3]
            iresultado = data.loc[(data[self.keycol] == ifiltro) & (data['METRICA'] == imetrica) & (data['TYPEOFKPI'] == itypeofkpi), self.periodo]
            if not(iresultado.empty):
                df.loc[i, self.periodo] = iresultado.values[0] # por defecto retorna series, el [0] es para que retorne un valor

        df.dropna(subset = [self.periodo], inplace = True)
        return df

    def setValues(self, col, values_detected, data):
        # icol, imetrica, itypeofki son parametros del query y lanza una advertencia que no se usan las variables pero no es así.
        
        # reordenando columnas
        values_detected = values_detected[[col, 'METRICA', 'TYPEOFKPI', self.periodo]]
        
        qry = col + ' == @icol & METRICA == @imetrica  & TYPEOFKPI == @itypeofkpi'
        
        # Importante, afectando sólo a las personas que tienen valores nulos. Si se omite esto, afecta a las posiciones y
        # reemplaza a personas que hayan tenido buenas lecturas
        df = data.copy()
        df = df[df[self.periodo].isnull()]
        #values_detected.to_csv('D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/'+ '_values_detected.csv')
        #df.to_csv('D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/'+ '_data.csv')

        for i in values_detected.index.values:
            icol = values_detected.ix[i].values[0]
            imetrica = values_detected.ix[i].values[1]
            itypeofkpi = values_detected.ix[i].values[2]
            iresultado = values_detected.ix[i].values[3]
            #print('Estos criterios {}, {}, {} tienen el resultado  {}'.format(icol,imetrica,itypeofkpi,iresultado)) # punto de control
            indices = df.query(qry)
            data.loc[indices.index.values, self.periodo] = iresultado

        #print(qry) # ---> punto de control
        return data
    
    def sumValues(self, logins, kpis, data):
            
        df = data[data['METRICA'].isin(kpis)]
        df = df[df[self.keycol].isin(logins)]
        df = df.groupby([self.keycol, 'TYPEOFKPI'], as_index=False).sum()
        df['METRICA'] = 'Gross Regiones'

        return df
    
    
    def superMerge(self, subsetcol, panelcol, subset, panel, comisionantes):
        """ Las posiciones deben ser identicas para hacer el merge"""
            
        
        # Conbinando comisionantes y kpis. Incluyendo Objetivos y Resultados  
        dfc = comisionantes.merge(subset, on = subsetcol, how = 'left')
        dfr = dfc.copy()
        dfc['TYPEOFKPI'] = 'objetivos'
        dfr['TYPEOFKPI'] = 'resultados'
        df = dfc.append(dfr)
        
        # Merging comisionantes y panel. Completando la data por nombre       
        df = df.merge(panel, on = [self.keycol, panelcol,'TYPEOFKPI'], how = 'left')
        df.dropna(subset = ['TIPO'], inplace = True)

        # Completando el panel ya que hay kpis que son cuotas por posición. Se completa el typeofkpi.
        #data.loc[data['TYPEOFKPI'].isnull(),'TYPEOFKPI'] = 'objetivos'
        
        return df
        
    def wrangler(self, data = None, results = None):
        """ data es objetivos o panel de plataformas """
        
        replvalues1 = 'Objetivo|objetivo|Cuota|cuota'
        replvalues2 = 'Obj.|Obj|obj|Ob.'
        replvalues3 = '(- .)$|(- )$|\(\)$|\(\.\)$'
        replvalues4 = 'Venta TPF' # esta secuencia es importante para detectar TPF antes de TP
        replvalues5 = 'Venta TP|Venta TC|Venta Islas'       
        objvalues = 'Obj|obj|Cuota|cuota'
        navalues = 'DIV/0!.*|.*REF.*|,|-|2da|2d%|.'
        new_kpi_superv = 'Ventas Totales'
                  
        if self.tipo == 'voz':
            objetivos = data.copy()
            resultados = results.copy()
            objetivos.rename(columns = {'DATOS':self.keycol}, inplace = True)
            resultados.rename(columns = {'DATOS':self.keycol}, inplace = True)
            objetivos['TYPEOFKPI'] = 'objetivos'
            resultados['TYPEOFKPI'] = 'resultados'
            df = objetivos.append(resultados)          
            df[self.periodo].replace(navalues, np.NaN, regex=True, inplace=True)
        
        elif self.tipo == 'plataformas':           
            df = data.copy()
            df.rename(columns = {'DATOS':self.keycol}, inplace = True)
            df['TYPEOFKPI'] = 'resultados'
            #df[self.periodo].replace(navalues, np.NaN, regex=True, inplace=True)
            df.loc[df[self.keycol].str.contains(objvalues, na=False),'TYPEOFKPI'] = 'objetivos'
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
        
        Por defecto aggfunc es mean pero hay valores nulos (como los nuevos ingresos), por ello se escoge         aggfunc='first'
        """

        caja_dic=[{'CAJA':'CAPTURA','NIVEL_CAJA':1},{'CAJA':'GESTIÓN','NIVEL_CAJA':2},{'CAJA':'DESARROLLO','NIVEL_CAJA':3}]
        
        df = panel.copy()
        df['CAJA'] = df['TIPO'].apply(lambda x: x[:len(x)-2])

        df_nivel_caja = pd.DataFrame(caja_dic)
        df = df.merge(df_nivel_caja, on = 'CAJA', how = 'left')
           
        #df.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+'kpis.csv') # Exportando Kpis
        #pivot = df.pivot_table(index = ['item', 'gerencia2','zona', 'posición',self.keycol], columns=['typeofkpi','nivel_caja','tipo'], values = self.periodo, aggfunc='first')
        #pivot = df.pivot_table(index = ['ITEM', 'GERENCIA2','ZONA', 'DEPARTAMENTO', 'POSICIÓN',self.keycol], columns=['TYPEOFKPI','NIVEL_CAJA','TIPO'], values = self.periodo, aggfunc='first')
        pivot = df.pivot_table(index = ['ITEM', 'GERENCIA2','ZONA', 'DEPARTAMENTO', 'KAM', 'POSICIÓN',self.keycol], columns=['TYPEOFKPI','NIVEL_CAJA','TIPO'], values = self.periodo, aggfunc='first')        
        return pivot
        
    def getSupervisor(self, canal, subpanel):
        
        df = self.metricasconj[self.metricasconj['area'] == canal].merge(subpanel, on = ['POSICIÓN', 'METRICA','TYPEOFKPI'], how = 'left')   
    
        # Reordenando columnas
        df = df[['NOMBRES', 'METRICA', 'TYPEOFKPI', 'POSICIÓN', 'KAM']]
        return df

class DataFramePreparation(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def prepareCols(self):
        pass

class HistoricalDataFrame(DataFramePreparation):
    
    def __init__(self, params):
        self.params = params
        
        
    def prepareCols(self):
        
        tipo = self.params['tipo']
        periodo = self.params['periodo']
        keycol = self.params['keycol']
        loginseq = self.params['logins']
        metricasconjuntas = self.params['metricasconjuntas']
        kpis = self.params['kpis']
        comisionantes = self.params['comisionantes']
        cuotas = self.params['cuotas']
        resultados = self.params['resultados'] 
        
        powercleaner = DataframeCleaner(keycol, tipo, periodo)        
        loginseq.rename(columns = {'LOGIN_EQUIVALENTE' : keycol}, inplace = True)
        kpis = powercleaner.kpiCleaner(kpis)

        panel = powercleaner.wrangler(cuotas, resultados)
 
        if tipo == 'voz':
            area = 'VENTAS DIRECTAS'
        elif tipo == 'plataformas':
            area = 'PLATAFORMAS COMERCIALES'
        
        #panel.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_panel1.csv') #---> Punto de Control
        panel = powercleaner.fillRows(kpis, panel, keycol, 'METRICA')
        
        #Asegurando el orden de las columnas del panel - Importante para usar el procedimiento rowsChange
        panel = panel[[keycol,'METRICA','TYPEOFKPI',periodo]]        
        
        panel = powercleaner.rowsChange(loginseq, panel, 'LOGIN_REAL', keycol)
        panel.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_panel2.csv') #---> Punto de Control        
        
        if tipo == 'plataformas':         
            panel = powercleaner.fillRows(comisionantes, panel, keycol, keycol)
            panel = panel[panel['DATOS'] == panel['METRICA']]
         
        #panel = panel.dropna() 
        panel.drop_duplicates(subset = [keycol, 'METRICA', 'TYPEOFKPI'], inplace = True)
        planilla = powercleaner.superMerge('POSICIÓN', 'METRICA', kpis, panel, comisionantes)
        panel.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_planilla1.csv') #---> Punto de Control  
              
        # Detectando las personas que están sin métrica. Completando datos por posición     
        nullpositions = planilla[planilla[periodo].isnull()][['POSICIÓN', 'KAM', 'METRICA', 'TYPEOFKPI']].drop_duplicates().reset_index(drop=True)
        valuestoset = powercleaner.detectValues(nullpositions, panel)
        planilla = powercleaner.setValues('POSICIÓN', valuestoset, planilla) 
        planilla.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_planilla2.csv')
        
        # Detectando las personas que están sin métrica.Llenando valores del supervisor al comisionante       
        nullrows = planilla[planilla[periodo].isnull()][['KAM', 'POSICIÓN', 'METRICA', 'TYPEOFKPI']].drop_duplicates().reset_index(drop=True)
        metricasconjuntas = metricasconjuntas[(metricasconjuntas['AREA']== area) & (metricasconjuntas['STATUS']=='Activo')]
        #metricasconjuntas.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ '_metricas_conjuntas.csv')
        listipofiltro = metricasconjuntas['VARIABLE'].drop_duplicates().tolist()
        metricsconjunt = pd.DataFrame()
        #print(listipofiltro)
        for filtro in listipofiltro:
            metrictemp = metricasconjuntas[metricasconjuntas['VARIABLE'] == filtro]
            metrictemp.rename(columns = {'VARIABLE_DATO' : filtro}, inplace = True)
            temp = metrictemp.merge(nullrows[[filtro,'METRICA','TYPEOFKPI']], on = [filtro, 'METRICA','TYPEOFKPI'], how = 'left')
            metricsconjunt = metricsconjunt.append(temp) 
        
        # Reordenando columnas
        metricsconjunt = metricsconjunt.drop_duplicates().reset_index(drop=True)  
        metricsconjunt = metricsconjunt[['NOMBRES', filtro, 'METRICA', 'TYPEOFKPI']]  
        #metricsconjunt.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ '_metricsconjunt.csv')     # ---> punto de control
        valuestoset = powercleaner.detectValues(metricsconjunt, panel)
        #valuestoset.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/'+ '_valuestoset.csv')     # ---> punto de control
        planilla = powercleaner.setValues(filtro, valuestoset, planilla)
                        
        df = powercleaner.powerPivot(planilla)
                
        # Dejando Logins equivalentes como antes
        loginseq.rename(columns = {keycol : 'LOGIN_EQUIVALENTE'}, inplace = True)
           
        return df
    
        
class PlainDataFrame(DataFramePreparation):
    
    # Dataframes que tienen columnas de fechas
    
    def __init__(self, params):
        self.params = params
        
    def prepareCols(self, data):      
        """ Convierte datetimes to string dates si usas sqlite """
        
        DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        ending = ['ACTIVACION' if self.params['section'] in ['tblEmpleados', 'tblVentaSSAA', 'tblPaquetes', 'tblVentas', 'tblJerarquia'] else 'DESACTIVACION' if self.params['section'] in ['tblDeacs', 'tblDeacSSAA'] else 'migracion']
        keyperiod = 'PERIODO_' + ending[0]
        
        df = data.copy()
        df['FECHA_ACTUALIZACION'] = datetime.now()
        
        #Agregando el periodo
        df[keyperiod] = df[self.params['colref']].map(lambda x: 100 * x.year + x.month)
        
        if self.params['section'] in ['tblVentas', 'tblDeacs']:
            df['FEC_DESACTIVA'] = pd.to_datetime(df['FEC_DESACTIVA'], dayfirst = True, coerce = True)
            if self.params['section'] == 'tblVentas':
                df['GROSSCOMISION'] = df['GROSSCOMERCIAL']
                df['CEDENTE']=df['CEDENTE'].fillna('No Determinado')
                df['PORTABILIDAD'] = df['CEDENTE'].apply(lambda x: 'Si' if x != 'No Determinado' else 'No')
                
        
        # Convirtiendo datetime to string
        for col in self.params['coldates']:
            df[col] = df[col].astype('O')
            df[col] = df[col].apply(lambda x: x.strftime(DATETIME_FORMAT) if pd.notnull(x) else '')

        return keyperiod, df

class OtherPlainDataFrame(DataFramePreparation):
    
        def __init__(self, params):
            self.params = params

        def prepareCols(self, section, data, periodo):
            
            if section == 'HC':
                df = data[data[self.params['colfilter']] == self.params['colfilteritem']]
                df = pd.DataFrame(df.groupby(self.params['colgroupby'])[self.params['colsum']].count()).reset_index()
                df['DATOS'] = df['DATOS'].apply(lambda x : 'KAM ' + x)
                
            # Insertando el nombre del kpi    
                d = {'DATOS' : pd.Series(['Cantidad de Consultores'], index = [0]), self.params['colsum'] : pd.Series([np.NaN], index = [0])}
                df = pd.DataFrame(d).append(df).reset_index(drop = True)
                       
            elif section == 'VAS':
                df = pd.DataFrame(df.groupby(self.params['colgroupby'])[self.params['colsum']].sum()).reset_index()
                d = {'DATOS' : pd.Series(['VAS'], index = [0]), self.params['colsum'] : pd.Series([np.NaN], index = [0])}
                df = pd.DataFrame(d).append(df).reset_index(drop = True)
           
            df.rename(columns = {self.params['colsum']:periodo}, inplace = True)
            
            return df