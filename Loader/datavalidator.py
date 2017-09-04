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
        
        nocolsjerarquia = ['nombre_vendedor','estado_vendedor','gerencia_1','gerencia_2','canal_de_venta','zona_de_venta',
                            'supervision_kam','departamento','codigo_inar']
        nocolsm2m = ['id_empl', 'codigo_inar', 'dni', 'apellido_paterno', 'nombres', 'apellido_materno', 'fecha_ingreso', 
                    'fecha_actualizacion', 'posicion_empl', 'periodo_activacion']
        
        # Limpiando el inar bruto
        df0 = self.inarbruto[self.inarbruto['estado'] != 'DEAC']
        df0 = df0[df0['vendedor']!= 'SERVICIO_GENERAL1']      
        
        #Corrige las Ñs
        df0 = df0.replace(',|\r|"|\x91', '', regex = True)
        df0[['vendedor','razon_social']] = df0[['vendedor','razon_social']].replace('Ã','Ñ', regex = True)
              
        # Empezando con las validaciones    
        blanksdoc = df0[(df0['tipodoc'].isnull()) & (~df0['plan_tarifario'].str.contains('Prepago'))]
        blanksdoc['observacion'] = 'blanks en documento'
        
        df0 = df0[df0['tipodoc'].isin(['RUC',np.NaN])]
        
        blanksvendedor = df0[(df0['vendedor'].isnull()) | (df0['zona'].isnull())]
        blanksvendedor['observacion'] = 'blanks en vendedor o zona'
        
        vendedorsinjerarquia = df0.merge(self.jerarquia.drop_duplicates(['codigo_inar']),left_on = ['vendedor'], 
                                        right_on = ['codigo_inar'], how = 'left' )
        vendedorsinjerarquia = vendedorsinjerarquia[vendedorsinjerarquia['gerencia_2'].isnull()]   
        vendedorsinjerarquia.drop(nocolsjerarquia, inplace=True, axis=1)    
        vendedorsinjerarquia['observacion'] = 'vendedor no se encuentra en jerarquia de ventas'
        
        # Detectando planes de data con consultores de voz
        
        emplactivos = self.tblempleados[self.tblempleados['fecha_cese'] == '']
        
        puestosvoz = emplactivos[~emplactivos['posicion_empl'].
                                 str.contains('Soluciones de Negocio')]['posicion_empl'].drop_duplicates()
        puestosvoz = puestosvoz.tolist()
        
        vozm2m = df0.merge(emplactivos, left_on = ['vendedor'], right_on = ['codigo_inar'], how = 'left' )
        vozm2m = vozm2m[vozm2m['plan_tarifario'].str.contains('M2M') & vozm2m['posicion_empl'].isin(puestosvoz)]
        vozm2m.drop(nocolsm2m, inplace = True, axis = 1)
        vozm2m['observacion'] = 'consultor de voz con planes data'
        
        # Detectando empleados inactivos
        emplcesados = self.tblempleados[self.tblempleados['fecha_cese'] != '']
        inactivos = df0.merge(emplcesados, left_on = ['vendedor'], right_on = ['codigo_inar'])
        inactivos.drop(nocolsm2m, inplace = True, axis = 1)
        inactivos['observacion'] = 'consultor cesado'
        
        df = blanksdoc.append(blanksvendedor, ignore_index = True)
        df = df.append(vendedorsinjerarquia, ignore_index = True)
        df = df.append(vozm2m, ignore_index = True)
        df = df.append(inactivos, ignore_index = True)
        
        df = df [['codigo','razon_social','contrato','fec_activ','fec_desactiva','estado','telefono','modelo','tmcode',
                  'plan_tarifario','vendedor','zona','dealer','tecnologia','tipodoc','documento','falso_deac','fecha_proceso',
                  'fecha_cese','observacion']]
        
        return df
    
class ValidateMultipleData(ValidateDataFrame):
    
    def __init__(self, dataframelist):

        self.jerarquia = dataframelist['jerarquia']
        self.comisionantes = dataframelist['comisionantes']
        self.tblempleados = dataframelist['tblempleados']
        
    def validation(self):      
        
        #importante renombrar la columna codigo_inar en jerarquia
        self.jerarquia.rename(columns = {'codigo_inar' : 'vendedor'}, inplace = True)
        
        tblempleados = self.tblempleados[['codigo_inar', 'fecha_cese', 'posicion_empl']]
        comisionantes = self.comisionantes[['login', 'gerencia2', 'zona', 'departamento', 'posición']]
        jerarquia = self.jerarquia[['canal_vista_negocio', 'gerencia_2', 'zona_de_venta', 'departamento', 'vendedor', 
                                    'estado_vendedor']]
        
        # Observaciones en jerarquia
        
        gerencia2 = ['CORPORACIONES','DESARROLLO NEGOCIOS PYME','GRANDES CLIENTES','SOLUCIONES DE DATOS',
                     'VD PYMES','VENTA REGIONAL EMPRESA']
        jerarquiadep = jerarquia[(jerarquia['canal_vista_negocio'] == 'EJECUTIVO ENTEL') & 
                                 (jerarquia['estado_vendedor'] == 'Activo')]
        jerarquiadep = jerarquiadep[jerarquiadep['gerencia_2'].isin(gerencia2)]
        
        emplcesados = tblempleados[(tblempleados['codigo_inar'] != '') & (tblempleados['fecha_cese'] != '')]
        
        vendedorinactivo = jerarquiadep.merge(emplcesados, left_on=['vendedor'],right_on=['codigo_inar'])
        vendedorinactivo['observacion'] = 'inactivar vendedor en jerarquia'
        vendedorinactivo.drop(['vendedor'], axis = 1, inplace =True)
             
        # Observaciones en comisionantes
        
        comisionantespos = comisionantes[comisionantes['posición'].str.contains('Consultor|Ejecutivo')]
        colscomisionantes = ['login','gerencia2','zona','departamento']
        concomisionantes = comisionantespos[colscomisionantes]
        concomisionantes['concolscom'] = concomisionantes.apply(lambda x : '|'.join(x), axis = 1) # no acepta columnas vacias
        
        colsjerarquia = ['vendedor','gerencia_2','zona_de_venta','departamento']
        conjerarquia = jerarquiadep[colsjerarquia] 
        
        conjerarquia['concolsjer'] = conjerarquia.apply(lambda x : '|'.join(x), axis = 1)
            
        comparezonas = concomisionantes.merge(conjerarquia, left_on = ['login'], right_on = ['vendedor'], how = 'left')
        comparezonas = comparezonas[comparezonas['concolscom'] != comparezonas['concolsjer']]
        comparezonas['observacion'] = 'diferencias en zonas'
        comparezonas.rename(columns = {'login':'codigo_inar','departamento_x':'departamento'}, inplace = True)
        #comparezonas.drop(['concolscom', 'concolsjer', 'vendedor'], axis = 1, inplace =True)
        
        # Observaciones en tblempleados
        
        tblsincodigos = jerarquiadep.merge(tblempleados, left_on = ['vendedor'], right_on = ['codigo_inar'], how = 'left')
        tblsincodigos = tblsincodigos[tblsincodigos['codigo_inar'].isnull()]
        tblsincodigos ['observacion'] = 'ingresar el codigo_inar en la tabla empleados'
        tblsincodigos['codigo_inar'] = tblsincodigos['vendedor']
        
        # Observaciones en la posición de comisionantes
        errorenpuesto = comisionantes.merge(tblempleados, left_on = ['login'], right_on = ['codigo_inar'])
        errorenpuesto = errorenpuesto[errorenpuesto['posición'] != errorenpuesto['posicion_empl']]
        errorenpuesto['observacion'] = 'actualizar el puesto en tblempleados'
            
        # Armando el df con las observaciones
        df = vendedorinactivo.append(comparezonas, ignore_index= True)
        df = df.append(tblsincodigos, ignore_index = True)
        df = df.append(errorenpuesto, ignore_index = True)
        df.drop(['canal_vista_negocio', 'estado_vendedor','posicion_empl', 'login', 'vendedor'], axis = 1, inplace =True)
        
        df = df[['codigo_inar','gerencia2','gerencia_2','zona','zona_de_venta','departamento','departamento_y','posición',
                 'concolscom','concolsjer','fecha_cese','observacion']]
 
        #importante devolver el nombre a codigo_inar en jerarquia
        self.jerarquia.rename(columns = {'vendedor' : 'codigo_inar'}, inplace = True)
        
        return df

      