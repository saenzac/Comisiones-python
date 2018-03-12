# -*- coding: utf-8 -*-
"""
Created on Sun Aug 13 16:00:01 2017

@author: Calobeto

"""
import abc
import pandas as pd
import os
from pandas import Series, DataFrame
from configparser import SafeConfigParser
import codecs
import ast
from Loader import datapreparation as dp
from datetime import datetime

class GenericInputFile(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def readFile(self):
        pass
    
    def display(self, paramsfile):
        print('El tamaño de %s es %s registros' % (paramsfile['section'], paramsfile['lenght']))
    
class ReadTxtFile(GenericInputFile):
    #Importación de archivo txts que tengan mismo nombre clave. Puede unir dos archivos de nombre similar y
    # convertirlos a dataframe
    
    def __init__(self, parameters):       
        self.parameters = parameters
        
    def readFile(self):
        # '\t' para espacios en blanco entre columnas
        
        defaultseparator = '|'
        colnames = self.parameters['cols']
        df = pd.DataFrame()
        for file in self.parameters['filelist']:
            df0 = pd.read_csv(file, sep = defaultseparator, usecols = colnames, encoding = 'latin-1',
                             dtype = {'CODIGO' : object, 'DOCUMENTO' : object})
            if 'BAM' in file:
                df0.loc[:,'TECNOLOGIA'] = 'BAM'
            df = df.append(df0, ignore_index = True)
        return df
        

class ReadExcelFile(GenericInputFile):
    #Importación de archivo xlsx que tengan mismo nombre clave. Puede unir varios archivos en carpetas distintas, unir
    # hojas

    def __init__(self, parameters):       
        self.parameters = parameters
        
    def readFile(self):
        # lee archivos de una lista, lee multiples hojas
        
        filelist = self.parameters['filelist']
        df = pd.DataFrame()
        converters = {col : str for col in self.parameters['strcols']}
        #print(self.parameters['cols'])
        for item in filelist:
            
            df0 = pd.read_excel(item, sheetname = self.parameters['presetsheet'], na_values = self.parameters['navalues'], skiprows = self.parameters['skiprows'], converters = converters)                               
            df = df.append(df0[self.parameters['cols']], ignore_index = True)
        
        #Eliminando Columnas y filas sin data
        df = df.dropna(axis = 1, how = 'all')
        df = df.dropna(how = 'all')

        return df

        
class ReadXlsxFile(GenericInputFile):
    #Importación de archivo xlsx que tengan mismo nombre clave. Puede unir varios archivos de carpetas distintas, unir hojas

    def __init__(self, parameters):       
        self.parameters = parameters
        
    def readFile(self):       
        # lee archivos de una lista, lee multiples hojas
        
        filelist = self.parameters['filelist']
        #"""        
        if self.parameters['section'] == 'Tracking':
            df = pd.DataFrame()
            df['Datos'] = ''
        else:    
            df = pd.DataFrame()
        #""" 
        #df = pd.DataFrame()
        
        for item in filelist:
 
            workbook = pd.ExcelFile(item)
            
            if self.parameters['presetsheet'] == '':
                sheets = workbook.sheet_names
            else:
                sheets = [self.parameters['presetsheet']]
        
            for sheet in sheets:

                if self.parameters['parsecols'] == 'None':
                    df0 = workbook.parse(sheetname = sheet, skiprows = self.parameters['skiprows'], na_values = self.parameters['navalues'])
                else:
                    df0 = workbook.parse(sheetname = sheet, skiprows = self.parameters['skiprows'], na_values = self.parameters['navalues'], parse_cols = self.parameters['parsecols'])
                
                #Eliminando Columnas y filas sin data
                #print(df0.columns) # punto de test
                
                df0 = df0.dropna(axis = 1, how = 'all')
                df0 = df0.dropna(how = 'all')

                if self.parameters['typeofinf'] == 'Historical' :
                    header = self.generateNewHeader(df0.columns.values)
                    df0.columns = header
                
                if self.parameters['section'] == 'Tracking':
                    df = df.merge(df0, on = 'Datos', how = 'right')                    

                else:
                    df = df.append(df0[self.parameters['cols']], ignore_index = True)
                           
        return df
       
    def generateNewHeader(self, columns):
        #Genera los encabezados segun formato
        #print(columns)
        newheader = []
        MONTH_HEADER = {1:'ene',2:'feb',3:'mar',4:'abr',5:'may',6:'jun',7:'jul',8:'ago',9:'sep',10:'oct',11:'nov',12:'dic'}
        
        #Removiendo 'Datos' temporalmente para trabajar en el formato de fechas
        #print('encabezado : %s'%columns) <-- Control
        dates = columns[1:]
     
        #print('dates : %s'%dates) # Punto de Test
        for m in dates:
            periodo = MONTH_HEADER[m.month] + '-' + str(m.year)[2:]
            newheader.append(periodo)
        
        #Reinsertando 'Datos' a new_header
        newheader = [columns[0]] + newheader

        return newheader        
        
        
class ReadIniFile(GenericInputFile):
    
    
    def __init__(self, inifile):
        
        self.inifile = inifile
    
    def readFile(self):
        
        parser = SafeConfigParser()

        with codecs.open(self.inifile, 'r', encoding='utf-8') as f:
            parser.readfp(f)

        return parser

class LoadFileProcess(object):
    
    def __init__(self, month):
        self.month = month
        self.parser = None
        self.section = None
        self.defaultpath = None
        self.parameters = None
        self.periodo = None
        
    def configParameters(self): # incluía parser
        """ Rutina que importa myconfig.ini y construye el dict"""
        """ https://pymotw.com/2/ConfigParser/ """
        """ http://stackoverflow.com/questions/335695/lists-in-configparser """
        """ En caso la data sea Historical se agrega el periodo a la lista cols"""

        yeardic = {'201701':'ene-17','201702':'feb-17','201703':'mar-17','201704':'abr-17','201705':'may-17','201706':'jun-17', 
                   '201707':'jul-17','201708':'ago-17','201709':'sep-17','201710':'oct-17','201711':'nov-17','201712':'dic-17',
                   '201801':'ene-18','201802':'feb-18','201803':'mar-18','201804':'abr-18','201805':'may-18','201806':'jun-18',
                   '201807':'jul-18','201808':'ago-18','201809':'sep-18','201810':'oct-18','201811':'nov-18','201812':'dic-18',}    

        if self.month:
            self.periodo = yeardic[self.month]
        
        #print(periodo)
        l2 = []

        for item in self.parser.options(self.section): # 
            if item == 'skiprows':
                l2.append(self.parser.getint(self.section,item)) # si la opcion es un entero
                                      
            elif item in ['cols', 'datadir', 'keyfile', 'defaultdir', 'colsconverted', 'colstochange', 'strcols', 'parsecols']:
                l2.append(ast.literal_eval(self.parser.get(self.section,item)))       # si la opcion es una lista
 
            else:
                l2.append(self.parser.get(self.section,item))

        self.parameters = dict(zip(self.parser.options(self.section),l2))

        keyfile = self.parameters['keyfile']
        if self.month :
            self.parameters['keyfile'] = [self.month + item for item in self.parameters['keyfile']]
            self.parameters['periodo'] = self.periodo

        if self.section == 'Logins' or self.section == 'Metricas_conjuntas':
            self.parameters['keyfile'] = keyfile

        if self.parameters['typeofinf'] == 'Historical':
            self.parameters['cols'].append(self.periodo)

        if not self.parameters['datadir'] :
            self.parameters['datadir'] = self.defaultpath
                
    def adjustDataframe(self, data):
        
        # Limpiando información

        df = data.copy()
        
        #df.columns = df.columns.str.lower() # Convierte los encabezados en minisculas
        df.columns = df.columns.str.upper() # Convierte los encabezados en mayúsculas
        df.columns = df.columns.str.replace(' ','_')
        df.columns = df.columns.str.replace('/','_')
        
        df.replace('"|&|\r', '', regex = True, inplace = True)

        return df
        
    
    def loadFile(self, section):
             
        self.section = section
        self.configParameters()
        #print(self.parameters)
        filelist = self.generateInputs()
        self.parameters['filelist'] = filelist
        self.parameters['section'] = section     
        
        if self.section == 'Inar_bruto':
            fileobj = ReadTxtFile(self.parameters)
            
        elif self.section in ['Ingresos', 'Ceses', 'Inar', 'Paquetes', 'Planillas', 'Comisionantes_voz', 'Comisionantes_plataformas']:
            fileobj = ReadExcelFile(self.parameters)
            
        else:
            fileobj = ReadXlsxFile(self.parameters)
        
        df0 = fileobj.readFile()

        df = self.adjustDataframe(df0)

        # cambio de nombres de columna 
        if self.parser.has_option(section, 'colstochange'):
            newcols = dict(zip(self.parameters['colsconverted'], self.parameters['colstochange']))
            df.rename(columns = newcols, inplace = True)

        df.name = section
        
        paramsfile = {'section' : self.section, 'lenght' : len(df)}
        fileobj.display(paramsfile)
        
        return df
        
    def loadHistoricalFile(self, section):
        
        self.section = section
        self.configParameters()
        
        filelist = self.generateInputs()
        self.parameters['filelist'] = filelist
        self.parameters['section'] = section

        fileobj = ReadXlsxFile(self.parameters)
        
        df0 = fileobj.readFile()
        
        df = self.adjustDataframe(df0)
        
        paramsfile = {'section' : self.section, 'lenght' : len(df)}
        fileobj.display(paramsfile)
           
        return df
        
    def prepareHistoricalFiles(self, params):
        
        dfobjhis = dp.HistoricalDataFrame(params)
        df = dfobjhis.prepareCols()
        return df
        
    def prepareOtherPlainFiles(self, parameters):
        
        criteria = {'VAS' : {'colgroupby' : ['GERENCIA2' , 'ZONAVENTA', 'DEPARTAMENTO', 'GANADO_POR_VOZ'], 'colsum' : ['GERENCIA2' , 'ZONAVENTA', 'DEPARTAMENTO', 'GANADO_POR_VOZ']},'HC' : {'colfilter' : 'CARGO', 'colfilteritem' : 'CONSULTOR', 'colgroupby' : 'DATOS', 'colsum' : 'VENDEDORES'}}
        listofdfs = {}
        
        periodo = parameters['periodo']
        params = parameters['frames']
        #print(params.keys())
        for section in params.keys():
            otherobj = dp.OtherPlainDataFrame(criteria[section])  
            df = otherobj.prepareCols(section, params[section], periodo)
            listofdfs[section] = df
            
        return listofdfs
             
    def setParser(self, parser):
        self.parser = parser
        
    def setDefaultPath(self, defaultpath):
        self.defaultpath = [defaultpath]
        
    def getPeriodo(self):
        #return self.parameters['periodo']
        return self.periodo
        
    def generateInputs(self):
        
        datadir = self.parameters['datadir']
        keyfile = self.parameters ['keyfile']
        filelist = []
        for directorio in datadir:
            for name in os.listdir(directorio):
                if any(s in name for s in keyfile) and (not '~$' in name):
                    file = os.path.join(directorio, name)
                    filelist.append(file)
        return filelist
        
    def getParameters(self):
        return self.parameters

