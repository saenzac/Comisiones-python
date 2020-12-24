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

    def fillRows(self, subset, panel, keycol, new_panel_col):
        """ 
        Permite detectar elementos de kpis en panel y autollena. 'METRICA' es la columna adicional que 
        se crea con el autollenado. 
        """
        panel_df = panel.copy()
        subset_df = subset.copy()

        lista_ele_subset = subset_df[new_panel_col].drop_duplicates().tolist()
        boolean_df = panel_df[keycol].isin(lista_ele_subset)
        panel_df_solo_las_encontradas = panel_df[ boolean_df ]
        #Nueva columna llamada 'METRICA' en el panel de cuotas y resultados.
        panel_df[new_panel_col] = panel_df_solo_las_encontradas [keycol]
        panel_df[new_panel_col] = panel_df[new_panel_col].ffill()

        return panel_df

    def rowsChange(self, logins, panel, keycol):      
        """ Busca el login equivalente en caso de logins no encontrados """

        panel_df = panel.copy()

        # Detectando los datos a cambiar
        lista_logins_eq = logins[keycol].drop_duplicates().tolist()
        boolean_df = panel_df[keycol].isin(lista_logins_eq)
        #Subdataframe contiene aquellos que figuran en el excel de logins equivalentees
        values_detected = panel_df[boolean_df]
        #Quitamos los duplicados
        values_detected = values_detected.drop_duplicates(subset = [keycol])
        for i in values_detected.index:
            r1 = values_detected.loc[i].values[0]
            r2 = logins.loc[logins[logins[keycol] == r1].index, 'LOGIN_REAL'].values
            #print(r2) # Punto de Control - Asegura que no hay duplicados en login equivalentes
            panel_df.loc[panel_df[keycol] == r1, keycol] = r2

        return panel_df

    #Devuelve la tabla con los nombres de los kpis de la pestaña Leyenda de los arhcivos de comisiones Pymes y G. Cuentas.
    def kpiCleaner(self, kpis):
        # Asegúrate que item este codificado. 1XXX para Kpis, 2XXX para pesos, 3XXX para Limitantes Mínimos y 4XXX para limitantes Máximos
        dft = kpis[kpis['ITEM']!='ITEM']
        dft = dft[~dft['ITEM'].isnull()]
        #Filtro de sólo KPIs menores a 2000, es decir la primera tabla de la pestaña "Leyenda"
        df = dft[dft['ITEM']<2000]

        #dft.columns.tolist() <- ['ITEM','GERENCIA2','ZONA','DEPARTAMENTO',...'CAPTURA_1', ...'DESARROLLO_4']
        #columnnames <- ['CAPTURA_1', ..., 'DESARROLLO_4']]
        columnnames = df.columns.tolist()[6:]
        dfi = pd.DataFrame()

        for column in columnnames:
            dfslice = df[['ITEM','ESQUEMA',column]]
            dftemp = dfslice.dropna()
            dftemp = dftemp.rename(columns={column:'METRICA'})
            dftemp['TIPO'] = column
            dfi = dfi.append(dftemp)

        df = dfi.reset_index(drop=True)
   
        return df


    """ 
        
        values_detected: Dataframe con los registros que tienen vacío el campo de 'periodo' en el panel 
                         de cuotas/resultados. Ejemplo de estructura:
        POSICION                                  | KAM   | METRICA | TYPEOFKPI
        GERENTE DE NEGOCIOS CARTERA CORPORACIONES | LIMA  | Churn   | objetivos
        data: Dataframe de cuotas/resultados en donde buscaremos por posición de empleado los registros nulos 
              de 'values_detected'
        Return: Devuelve el dataframe de 'values_detected' con una columna adicional que contiene
                el valor hallado.
    """
    def detectValues(self, values_detected, data):
        df = values_detected.copy()
        df[self.periodo] = None

        # Iteramos por cada fila de 'values_detected' y buscamos el valor de la columna
        # 'POSICION' de 'values_detected' en la columna 'LOGINS' del dataframe data (panel de cuotas/resultados)
        # Si hay match entonces dicho valor se actualiza en 'values_detected' para luego ser seteado con la funcion self.setValues()
        for i in values_detected.index.values:
            #ifiltro = values_detected.ix[i].values[0]
            #imetrica = values_detected.ix[i].values[2]
            #itypeofkpi = values_detected.ix[i].values[3]
            ifiltro = values_detected.loc[i].values[0]
            imetrica = values_detected.loc[i].values[2]
            itypeofkpi = values_detected.loc[i].values[3]            
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
            #icol = values_detected.ix[i].values[0]
            #imetrica = values_detected.ix[i].values[1]
            #itypeofkpi = values_detected.ix[i].values[2]
            #iresultado = values_detected.ix[i].values[3]
            icol = values_detected.loc[i].values[0]
            imetrica = values_detected.loc[i].values[1]
            itypeofkpi = values_detected.loc[i].values[2]
            iresultado = values_detected.loc[i].values[3]
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
    
    def superMerge(self, kpiscol, panelcol1, panelcol2, kpis, panel, comisionantes):
        #Merge de DF Comisionates con DF kpis que contiene el nombre de los kpis y con DF Panel que contiene
        #los valores numericos del formato de cuotas y resultados.

        #dfc <-
        #        LOGIN                GERENCIA2                    ZONA  ...  ITEM                   METRICA          TIPO
        #0    MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...  1204                    VENTAS     CAPTURA_1
        #1    MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...  1204    % CUMPLIMIENTO CORREOS     GESTIÓN_1
        #2    MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...  1204  EFECTIVIDAD DE RETENCIÓN     GESTIÓN_2
        #3    MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...  1204                       NPS     GESTIÓN_3
        #4    MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...  1204         MIGRACIONES NETAS  DESARROLLO_1
        #..        ...                      ...                     ...  ...   ...                       ...           ...
        df_objetivos = comisionantes.merge(kpis, on = kpiscol, how = 'left')
        df_resultados = dfc.copy()
        df_objetivos['TYPEOFKPI'] = 'objetivos'
        df_resultados['TYPEOFKPI'] = 'resultados'

        df = df_objetivos.append(df_resultados)

        #df <- 
        #         LOGIN                GERENCIA2                    ZONA  ...   TYPEOFKPI                     DATOS    NOV-20
        #0     MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...   objetivos                    VENTAS         1
        #1     MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...   objetivos    % CUMPLIMIENTO CORREOS      0.85
        #2     MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...   objetivos  EFECTIVIDAD DE RETENCIÓN       742
        #3     MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...   objetivos                       NPS  0.547107
        #4     MMDAVILA  PLATAFORMAS COMERCIALES  PLATAFORMA CARTERIZADA  ...   objetivos         MIGRACIONES NETAS         1
        #...        ...                      ...                     ...  ...         ...                       ...       ...
        #1207    SPARE4                    SPARE                   SPARE  ...  resultados                   SPARE11       0.1
        #1208    SPARE4                    SPARE                   SPARE  ...  resultados                   SPARE12       0.1
        df = df.merge(panel, on = [panelcol1, panelcol2,'TYPEOFKPI'], how = 'left')
        df.dropna(subset = ['TIPO'], inplace = True)

        return df

    def wrangler(self, objetivos_ = None, resultados_ = None):
        """ data es objetivos o panel de plataformas """
        replvalues1 = 'Objetivo|objetivo|Cuota|cuota'
        replvalues2 = 'Obj.|Obj|obj|Ob.|OBJ.|OBJ'
        replvalues3 = '(- .)$|(- )$|\(\)$|\(\.\)$'
        replvalues4 = 'Venta TPF' # esta secuencia es importante para detectar TPF antes de TP
        replvalues5 = 'Venta TP|Venta TC|Venta Islas'       
        objvalues = 'Obj|OBJ|obj|Cuota|cuota'
        navalues = 'DIV/0!.*|.*REF.*|,|-|2da|2d%|.'
        new_kpi_superv = 'Ventas Totales'

        #Si el tipo es plataformas en el dataframe objetivos_ vienen tanto objetivos como resultados.           
        objetivos = objetivos_.copy()
        if resultados_ != None:
            resultados = resultados_.copy()

        if self.tipo == 'voz':
            objetivos.rename(columns = {'DATOS':self.keycol}, inplace = True)
            objetivos['TYPEOFKPI'] = 'objetivos'
            resultados.rename(columns = {'DATOS':self.keycol}, inplace = True)
            resultados['TYPEOFKPI'] = 'resultados'
            ret_df = objetivos.append(resultados)
            #Limpieamos la columna del periodo elegido
            ret_df[self.periodo].replace(navalues, np.NaN, regex=True, inplace=True)

        elif self.tipo == 'plataformas':
            data = objetivos
            data.rename(columns = {'DATOS':self.keycol}, inplace = True)
            data['TYPEOFKPI'] = 'resultados'
            #Localizamos los registros que tienen 'Obj.|OBJ...' y en una columna nueva llamada
            #'TYPEOFKPI' las marcamos con 'objetivos'
            objetivos_boolean_series = objetivos[self.keycol].str.contains(objvalues, na=False)
            data.loc[objetivos_boolean_series ,'TYPEOFKPI'] = 'objetivos'
            data[self.keycol] = data[self.keycol].str.replace(replvalues1, "")
            #Elimina la cadena "Obj." de la columna <keycol> antes llamada DATOS
            data[self.keycol] = data[self.keycol].str.replace(replvalues2, "")
            data[self.keycol] = data[self.keycol].str.replace(replvalues3, "")
            data[self.keycol] = data[self.keycol].str.replace(replvalues4, new_kpi_superv)
            data[self.keycol] = data[self.keycol].str.replace(replvalues5, new_kpi_superv)
            data[self.keycol] = data[self.keycol].str.rstrip()
            ret_df = data

        ret_df.reset_index(drop = True)       
        return ret_df  
    
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
        pivot = df.pivot_table(index = ['ITEM', 'GERENCIA2','ZONA', 'DEPARTAMENTO', 'KAM', 'ESQUEMA',self.keycol], columns=['TYPEOFKPI','NIVEL_CAJA','TIPO'], values = self.periodo, aggfunc='first')
        return pivot
        
    def getSupervisor(self, canal, subpanel):
        
        df = self.metricasconj[self.metricasconj['area'] == canal].merge(subpanel, on = ['ESQUEMA', 'METRICA','TYPEOFKPI'], how = 'left')
    
        # Reordenando columnas
        df = df[['NOMBRES', 'METRICA', 'TYPEOFKPI', 'ESQUEMA', 'KAM']]
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
        tipo = self.params['tipo'] #Por ejemplo 'Voz'
        periodo = self.params['periodo'] # Por ejemplo 'ene-18' pero en mayusculas.
        keycol = self.params['keycol'] #Por ejemplo 'LOGIN'
        loginseq = self.params['logins'] #Dataframe  archivo de logins equivalentes
        loginseq.rename(columns={'LOGIN_EQUIVALENTE': keycol}, inplace=True)
        metricasconjuntas = self.params['metricasconjuntas'] #Dataframe de metricas conjuntas
        #Nota: Obligatorio que kpis previamente este convertido a mayusculas.
        kpis = self.params['kpis'] #Dataframe de pestaña leyendo de los archivos de comisiones Pymes y Grandes Clientes
        comisionantes = self.params['comisionantes'] #Dataframe de pestaña comisionantes
        cuotas = self.params['cuotas'] #Dataframe de archivo de 'formato de cuotas'
        resultados = self.params['resultados'] #Dataframde archivo de 'formato de resultados'

        powercleaner = DataframeCleaner(keycol, tipo, periodo)

        # Transforma tabla de pestaña 'Leyenda' a :  kpis <-
        #     ITEM                             ESQUEMA          METRICA          TIPO
        #0    1204    JEFE DE PLATAFORMAS CARTERIZADAS           VENTAS     CAPTURA_1
        #1    1205         JEFE DE PLATAFORMAS INBOUND           VENTAS     CAPTURA_1
        #2    1206        ASESOR EMPRESAS PLUS INBOUND           VENTAS     CAPTURA_1
        #3    1207  SUPERVISOR SERVICIOS INBOUND IH IB           VENTAS     CAPTURA_1
        #4    1218               ASESOR REDES SOCIALES           VENTAS     CAPTURA_1
        #..    ...                                 ...              ...           ...
        #126  1205         JEFE DE PLATAFORMAS INBOUND  CHURN RUC TOTAL  DESARROLLO_3
        #127  1208      ASESOR EMPRESAS PLUS RETENCIÓN   CHURN RUC PYME  DESARROLLO_3
        #...
        kpis_df = powercleaner.kpiCleaner(kpis)

        # * Carga DF de panel de cuotas y resultados
        # panel_cuotas_y_result_df <-
        # VOZ:
        # <keycol> = LOGINS            | ABR-19   | TYPEOFKPI
        # Ventas S/                    | nan      | objetivos
        # DCANALE                      | 21305.0  | objetivos
        # EJECUTIVO DE DESARROLLO PYME | 40       | objetivos
        #     ...                      |  ...     |   ...
        # A.CHONYEN                    | 44.64    | resultados

        # PLATAFORMAS:       
        # <keycol> = NOMBRES           | ABR-19   | TYPEOFKPI
        # <Nombre ejecutivo>           | nan      | nan
        # Migraciones                  | 21305.0  | resultados
        # Migraciones                  | 92322.3  | objetivos
        #     ...                      |  ...     |   ...
        panel_obj_y_result_df = powercleaner.wrangler(cuotas, resultados)
 
        if tipo == 'voz':
            area = 'VENTAS DIRECTAS'
        elif tipo == 'plataformas':
            area = 'PLATAFORMAS COMERCIALES'

        # * Completa con el nombre del kpi las celdas inferiores subyacentes hasta encontrar otro kpi  en donde el ciclo vuelve a comenzar.
        # * Esto se da en una nueva columna llamada 'METRICA'
        # <keycol>                     | METRICA        | TYPEOFKPI  | ABR-19
        # Ventas S/                    | Ventas S/      | objetivos  | nan
        # DCANALE                      | Ventas S/      | objetivos  | 21305.0
        # EJECUTIVO DE DESARROLLO PYME | Ventas S/      | objetivos  | 40
        #     ...                      |  ...           |   ...      | ...
        # A.CHONYEN                    | HC VENDEDORES  | resultados | 44.64
        panel_obj_y_result_df = powercleaner.fillRows(kpis_df, panel_obj_y_result_df, keycol,'METRICA')
        # Asegurando el orden de las columnas del panel - Importante para usar el procedimiento rowsChange
        panel_obj_y_result_df = panel_obj_y_result_df[[keycol,'METRICA','TYPEOFKPI',periodo]]    

        #Corrige los logins por los equivalentes
        #loginseq <-
        #  LOGIN_EQUIVALENTE -> <keycol>	| LOGIN_REAL	 | AREA
        #  CORPORACIONES1	                | PDIAZ	         | CORPORACIONES
        #Fiorella Asian Lombardi            | Fiorella Asian | PLATAFORMAS COMERCIALES
        panel_obj_y_result_df = powercleaner.rowsChange(loginseq, panel_obj_y_result_df, keycol)
        #panel.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_panel2.csv') #---> Punto de Control

        if tipo == 'plataformas':
            #Agregamos una nueva columna llamanda nombres.
            #panel_obj_y_result_df <-
            #                        DATOS                 METRICA   TYPEOFKPI    NOV-20          NOMBRES
            #1                        NPS                     NPS  resultados  0.530595  MILAGROS DÁVILA
            #2                        NPS                     NPS   objetivos  0.547107  MILAGROS DÁVILA
            #9          EFECTIVIDAD TOTAL       EFECTIVIDAD TOTAL  resultados  0.901087  MILAGROS DÁVILA
            #10         EFECTIVIDAD TOTAL       EFECTIVIDAD TOTAL   objetivos  0.913333  MILAGROS DÁVILA
            #17    % CUMPLIMIENTO CORREOS  % CUMPLIMIENTO CORREOS  resultados  0.931786  MILAGROS DÁVILA
            #...                      ...                     ...         ...       ...              ...         
            panel_obj_y_result_df = powercleaner.fillRows(comisionantes, panel_obj_y_result_df, keycol, 'NOMBRES')
            #Dejamos unicamente los resultados y objetivos, eliminamos lo demas.
            boolean_ = panel_obj_y_result_df[keycol] == panel_obj_y_result_df['METRICA']
            panel_obj_y_result_df = panel_obj_y_result_df[boolean_]

        panel_obj_y_result_df.drop_duplicates(subset = [keycol, 'METRICA', 'TYPEOFKPI'], inplace = True)

        # Merge de comisionantes , nombres de kpis y paneles de cuotas/resultados
        planilla = powercleaner.superMerge(kpiscol='ESQUEMA', panelcol1='METRICA', panelcol2='NOMBRES', 
                                           kpis=kpis_df, panel=panel_obj_y_result_df, comisionantes=comisionantes)
        #panel.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_planilla1.csv') #---> Punto de Control  

        #* Detectando las personas que están sin métrica. Completando datos por posición
        # Devuelve los registros que estan vacios de la columna 'periodo',
        # (es decir los LOGINS que no se hallaron en el formato de cuotas/resultados al hacer el superMerge)
        # Capturamos unicamente las columnas  ['POSICIÓN', 'KAM', 'METRICA', 'TYPEOFKPI'], no tomamos la de LOGIN
        # POSICION                                  | KAM   | METRICA | TYPEOFKPI
        # GERENTE DE NEGOCIOS CARTERA CORPORACIONES | LIMA  | Churn   | objetivos
        nullpositions = planilla[planilla[periodo].isnull()][['ESQUEMA', 'KAM', 'METRICA', 'TYPEOFKPI']].drop_duplicates().reset_index(drop=True)
        #
        valuestoset = powercleaner.detectValues(nullpositions, panel)
        planilla = powercleaner.setValues('ESQUEMA', valuestoset, planilla)
        #planilla.to_csv('D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Data Fuente Comisiones/test/' + '_planilla2.csv')
        
        # Detectando las personas que están sin métrica.Llenando valores del supervisor al comisionante       
        nullrows = planilla[planilla[periodo].isnull()][['KAM', 'ESQUEMA', 'METRICA', 'TYPEOFKPI']].drop_duplicates().reset_index(drop=True)
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
        # Convertimos el multilevel pivot a plain/flat.
        flattened = pd.DataFrame(df.to_records())
        if tipo == 'voz':
            logins = comisionantes[['LOGIN']]
            dfc = logins.merge(flattened, on="LOGIN", how='left')
        else:
            nombres = comisionantes[['NOMBRES']]
            # hacemos merge del pivot aplanado con los nombres del archivo de comisiones, para mantener el orden.
            dfc = nombres.merge(flattened, on="NOMBRES", how='left')

        return dfc
    
        
class PlainDataFrame(DataFramePreparation):
    
    # Dataframes que tienen columnas de fechas
    
    def __init__(self, params):
        self.params = params
        
    def prepareCols(self, data):      
        """ Convierte datetimes to string dates si usas sqlite """

        DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

        #if self.params['section'] in ['tblINAR','tblEmpleados', 'tblVentaSSAA', 'tblVentaSSAANew', 'tblPaquetes', 'tblVentas', 'tblVentasPersonas', 'tblJerarquia', 'tblGarantias','tblChurn','InsertarPadronEmpleados']:
        #    ending = ['ACTIVACION']
        #elif self.params['section'] in ['tblDeacs', 'tblDeacSSAA']:
        #    ending = ['DESACTIVACION']
        ##elif self.params['section'] in ['tblIndicadores1', 'tblIndicadores2']:
        #   ending = ['PRODUCCION']
        #else:
        #    ending = ['migracion']
        
        #keyperiod = 'PERIODO_' + ending[0]
        keyperiod = 'PERIODO_'

        df = data.copy()
        df['FECHA_ACTUALIZACION'] = datetime.now()

        #Agregando el periodo
        df[keyperiod] = df[self.params['colref']].map(lambda x: 100 * x.year + x.month)
        
        if self.params['section'] in ['tblVentas', 'tblDeacs']:
            df['FEC_DESACTIVA'] = pd.to_datetime(df['FEC_DESACTIVA'], dayfirst=True, errors='coerce')
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
            
            df = pd.DataFrame()

            if section == 'HC':
                df = data[data[self.params['colfilter']] == self.params['colfilteritem']]
                df = pd.DataFrame(df.groupby(self.params['colgroupby'])[self.params['colsum']].count()).reset_index()
                df['DATOS'] = df['DATOS'].apply(lambda x : 'KAM ' + x)
                
            # Insertando el nombre del kpi    
                d = {'DATOS' : pd.Series(['Cantidad de Consultores'], index = [0]), self.params['colsum'] : pd.Series([np.NaN], index = [0])}
                df = pd.DataFrame(d).append(df).reset_index(drop = True)

            """             
            elif section == 'VAS':
                
                for ambito in self.params['colgroupby']:
                    df = pd.DataFrame(df.groupby(self.params[ambito])[self.params['colsum']].sum()).reset_index()
                
            # Insertando el nombre del kpi 
                d = {'DATOS' : pd.Series(['VAS'], index = [0]), self.params['colsum'] : pd.Series([np.NaN], index = [0])}
                df = pd.DataFrame(d).append(df).reset_index(drop = True)
            """
            df.rename(columns = {self.params['colsum']:periodo}, inplace = True)
            
            return df

