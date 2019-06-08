# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 21:31:25 2017

@author: Calobeto
"""
import abc
import numpy as np
from pandas import ExcelWriter

class ValidateDataFrame(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def validation(self):
        pass
    
    def exportValidation(self, exportparams):
        writer = ExcelWriter(exportparams['xlsxfile'])
        exportparams['dataframe'].to_excel(writer, index = False, sheet_name = 'validaciones')
        writer.save()
        self.display(exportparams)
        
    def display(self, exportparams):
        print('Archivo exportado %s con %s registros' % (exportparams['xlsxfile'], len(exportparams['dataframe'])))
    
class ValidateInar(ValidateDataFrame):
    
    def __init__(self, dataframelist):
        self.inarbruto = dataframelist['inarbruto']
        self.jerarquia = dataframelist['jerarquia']
        self.comisionantes = dataframelist['comisionantes']
        self.tblempleados = dataframelist['tblempleados']
        
    def validation(self):      
        # Validación de blanks : tipodoc, vendedor, zona, tipodoc
        
        nocolsjerarquia = ['NOMBREVENDEDOR','ESTADOVENDEDOR','GERENCIA1','GERENCIA2','CANALDEVENTA','ZONAVENTA',
                            'SUPERVISORKAM','DEPARTAMENTO','VENDEDOR']
        nocolsm2m = ['ID_EMPL', 'CODIGO_INAR', 'DNI', 'APELLIDO_PATERNO', 'NOMBRES', 'APELLIDO_MATERNO', 'FECHA_INGRESO', 
                    'FECHA_ACTUALIZACION', 'POSICION_EMPL', 'PERIODO_ACTIVACION']
        
        # Limpiando el inar bruto
        df0 = self.inarbruto[self.inarbruto['ESTADO'] != 'DEAC']
        df0 = df0[df0['VENDEDOR']!= 'SERVICIO_GENERAL1']      
        
        #Corrige las Ñs
        df0 = df0.replace(',|\r|"|\x91', '', regex = True)
        df0[['VENDEDOR','RAZON_SOCIAL']] = df0[['VENDEDOR','RAZON_SOCIAL']].replace('Ã','Ñ', regex = True)
              
        # Empezando con las validaciones    
        blanksdoc = df0[(df0['TIPODOC'].isnull()) & (~df0['PLAN_TARIFARIO'].str.contains('Prepago'))]
        blanksdoc['OBSERVACION'] = 'blanks en documento'
        
        df0 = df0[df0['TIPODOC'].isin(['RUC',np.NaN])]
           
        blanksvendedor = df0[(df0['VENDEDOR'].isnull()) | (df0['ZONA'].isnull())]
        blanksvendedor['OBSERVACION'] = 'blanks en vendedor o zona'
        
        vendedorsinjerarquia = df0.merge(self.jerarquia.drop_duplicates(['VENDEDOR']), on = ['VENDEDOR'], how = 'left' )
        vendedorsinjerarquia = vendedorsinjerarquia[vendedorsinjerarquia['GERENCIA2'].isnull()]   
        vendedorsinjerarquia.drop(nocolsjerarquia, inplace=True, axis=1)    
        vendedorsinjerarquia['OBSERVACION'] = 'vendedor no se encuentra en jerarquia de ventas'
        
        # Detectando planes de data con consultores de voz
        
        emplactivos = self.tblempleados[self.tblempleados['FECHA_CESE'] == '']
        
        puestosvoz = emplactivos[~emplactivos['POSICION_EMPL'].str.contains('SOLUCIONES DE NEGOCIO')]['POSICION_EMPL'].drop_duplicates()
        puestosvoz = puestosvoz.tolist()
        
        vozm2m = df0.merge(emplactivos, left_on = ['VENDEDOR'], right_on = ['CODIGO_INAR'], how = 'left' )
        vozm2m = vozm2m[vozm2m['PLAN_TARIFARIO'].str.contains('M2M') & vozm2m['POSICION_EMPL'].isin(puestosvoz)]
        vozm2m.drop(nocolsm2m, inplace = True, axis = 1)
        vozm2m['OBSERVACION'] = 'consultor de voz con planes data'
        
        # Detectando empleados inactivos
        emplcesados = self.tblempleados[self.tblempleados['FECHA_CESE'] != '']
        inactivos = df0.merge(emplcesados, left_on = ['VENDEDOR'], right_on = ['CODIGO_INAR'])
        inactivos.drop(nocolsm2m, inplace = True, axis = 1)
        inactivos['OBSERVACION'] = 'consultor cesado'
        
        df = blanksdoc.append(blanksvendedor, ignore_index = True)
        df = df.append(vendedorsinjerarquia, ignore_index = True)
        df = df.append(vozm2m, ignore_index = True)
        df = df.append(inactivos, ignore_index = True)
        
        df = df [['CODIGO','RAZON_SOCIAL','CONTRATO','FEC_ACTIV','FEC_DESACTIVA','ESTADO','TELEFONO','MODELO','TMCODE',
                  'PLAN_TARIFARIO','VENDEDOR','ZONA','DEALER','TECNOLOGIA','TIPODOC','DOCUMENTO','FALSO_DEAC','FECHA_PROCESO',
                  'FECHA_CESE','OBSERVACION']]
        
        return df
    
class ValidateMultipleData(ValidateDataFrame):
    
    def __init__(self, dataframelist):

        self.jerarquia = dataframelist['jerarquia']
        self.comisionantes = dataframelist['comisionantes']
        self.tblempleados = dataframelist['tblempleados']
        
    def validation(self):      
        
        #importante renombrar la columna codigo_inar en jerarquia
        #self.jerarquia.rename(columns = {'codigo_inar' : 'vendedor'}, inplace = True)
        
        tblempleados = self.tblempleados[['CODIGO_INAR', 'FECHA_CESE', 'POSICION_EMPL']]
        comisionantes = self.comisionantes[['LOGIN', 'GERENCIA2', 'ZONA', 'DEPARTAMENTO', 'POSICIÓN']]
        jerarquia = self.jerarquia[['CANALVISTANEGOCIO', 'GERENCIA2', 'ZONAVENTA', 'DEPARTAMENTO', 'VENDEDOR', 
                                    'ESTADOVENDEDOR']]
        
        # Observaciones en jerarquia
        
        gerencia2 = ['CORPORACIONES','DESARROLLO NEGOCIOS PYME','GRANDES CLIENTES','SOLUCIONES DE DATOS',
                     'VD PYMES','VENTA REGIONAL EMPRESA']
        jerarquiadep = jerarquia[(jerarquia['CANALVISTANEGOCIO'] == 'EJECUTIVO ENTEL') & 
                                 (jerarquia['ESTADOVENDEDOR'] == 'Activo')]
        jerarquiadep = jerarquiadep[jerarquiadep['GERENCIA2'].isin(gerencia2)]
        
        emplcesados = tblempleados[(tblempleados['CODIGO_INAR'] != '') & (tblempleados['FECHA_CESE'] != '')]
        
        vendedorinactivo = jerarquiadep.merge(emplcesados, left_on=['VENDEDOR'],right_on=['CODIGO_INAR'])
        vendedorinactivo['OBSERVACION'] = 'inactivar vendedor en jerarquia'
        vendedorinactivo.drop(['VENDEDOR'], axis = 1, inplace =True)
             
        # Observaciones en comisionantes
        
        comisionantespos = comisionantes[comisionantes['POSICIÓN'].str.contains('CONSULTOR|EJECUTIVO')]
        colscomisionantes = ['LOGIN','GERENCIA2','ZONA','DEPARTAMENTO']
        concomisionantes = comisionantespos[colscomisionantes]
        concomisionantes['CONCOLSCOM'] = concomisionantes.apply(lambda x : '|'.join(x), axis = 1) # no acepta columnas vacias
        
        colsjerarquia = ['VENDEDOR','GERENCIA2','ZONAVENTA','DEPARTAMENTO']
        conjerarquia = jerarquiadep[colsjerarquia] 
        
        conjerarquia['CONCOLSJER'] = conjerarquia.apply(lambda x : '|'.join(x), axis = 1)
            
        comparezonas = concomisionantes.merge(conjerarquia, left_on = ['LOGIN'], right_on = ['VENDEDOR'], how = 'left')
        comparezonas = comparezonas[comparezonas['CONCOLSCOM'] != comparezonas['CONCOLSJER']]
        comparezonas['OBSERVACION'] = 'diferencias en zonas'
        comparezonas.rename(columns = {'LOGIN':'CODIGO_INAR','DEPARTAMENTO_X':'DEPARTAMENTO'}, inplace = True)
        #comparezonas.drop(['concolscom', 'concolsjer', 'vendedor'], axis = 1, inplace =True)
        
        # Observaciones en tblempleados
        
        tblsincodigos = jerarquiadep.merge(tblempleados, left_on = ['VENDEDOR'], right_on = ['CODIGO_INAR'], how = 'left')
        tblsincodigos = tblsincodigos[tblsincodigos['CODIGO_INAR'].isnull()]
        tblsincodigos ['OBSERVACION'] = 'ingresar el codigo_inar en la tabla empleados'
        tblsincodigos['CODIGO_INAR'] = tblsincodigos['VENDEDOR']
        
        # Observaciones en la posición de comisionantes
        errorenpuesto = comisionantes.merge(tblempleados, left_on = ['LOGIN'], right_on = ['CODIGO_INAR'])
        errorenpuesto = errorenpuesto[errorenpuesto['POSICIÓN'] != errorenpuesto['POSICION_EMPL']]
        errorenpuesto['OBSERVACION'] = 'actualizar el puesto en tblempleados'
            
        # Armando el df con las observaciones
        df = vendedorinactivo.append(comparezonas, ignore_index= True)
        df = df.append(tblsincodigos, ignore_index = True)
        df = df.append(errorenpuesto, ignore_index = True)

        df.drop(['CANALVISTANEGOCIO', 'ESTADOVENDEDOR','POSICION_EMPL', 'LOGIN', 'VENDEDOR'], axis = 1, inplace =True)
        
        df = df[['CODIGO_INAR','GERENCIA2','GERENCIA2_x','ZONA','ZONAVENTA','DEPARTAMENTO','DEPARTAMENTO_y','POSICIÓN',
                 'CONCOLSCOM','CONCOLSJER','FECHA_CESE','OBSERVACION']]
 
        #importante devolver el nombre a codigo_inar en jerarquia
        #self.jerarquia.rename(columns = {'vendedor' : 'codigo_inar'}, inplace = True)
        
        return df

      