# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 13:04:16 2017

@author: calobeto
"""
import ast
import abc
import sys
import sqlite3
import pandas as pd
from pandas import Series, DataFrame
from Loader import datapreparation as dp
from Loader import datacompute as dc

class DbGenericOperator(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def openDb(self):
        raise NotImplementedError
    
    @abc.abstractmethod
    def closeDb(self):
        raise NotImplementedError

    @abc.abstractmethod
    def readTbl(self,query):
        raise NotImplementedError
    
    @abc.abstractmethod
    def writeTbl(self,query):
        raise NotImplementedError      

    @abc.abstractmethod    
    def updateTbl(self):
        raise NotImplementedError
  
    @abc.abstractmethod
    def deleteTbl(self,query):
        raise NotImplementedError
        
class DbSqLiteOperator(DbGenericOperator):


    def __init__(self, params):

        self.dbpath = params['dbpath']
        self.dbname = params['dbname']

    def openDb(self):
        self.conn = sqlite3.connect(self.dbpath + self.dbname, detect_types = sqlite3.PARSE_DECLTYPES)
        
    def closeDb(self):
        self.conn.close()
        
    def readTbl(self, query):
        df = pd.read_sql_query(query, self.conn)
        return df
     
    def writeTbl(self, sql, tuplas):
        cursor = self.conn.cursor()
        cursor.executemany(sql, tuplas)
        self.conn.commit()
        cursor.close()
        
    def updateTbl(self, query):      
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()
        
    def deleteTbl(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
         
         
class DbDataProcess(object):
    
    def __init__(self, month):
        self.month = month
        self.parser = None
        self.section = None
        self.parameters = None
        self.dbpath = 'D:/Datos de Usuario/cleon/Documents/Mercado Empresas/Bases/'
        self.dbname = 'comisiones3.sqlite'
        
     
    def display(self, paramstable):
        print('Los registros de la tabla %s es %s registros %s ' % ((paramstable['section'], paramstable['lenght'], paramstable['comment'])))  
        
    def configParameters(self): # incluía parser
   
        l2 = []
        
        itemlist = ['coldates', 'colstoupdate', 'criterycols', 'dropcols']
    
        for item in self.parser.options(self.section): # 
            if item in itemlist:
                l2.append(ast.literal_eval(self.parser.get(self.section,item)))      
            else:
                l2.append(self.parser.get(self.section,item))

        self.parameters = dict(zip(self.parser.options(self.section),l2))

        self.parameters['section'] = self.section
        self.parameters['dbpath'] = self.dbpath
        self.parameters['dbname'] = self.dbname
                
    
    def loadData(self, section):
        
        self.section = section
        self.configParameters()
        if section in ['Gross_Comision','Reversiones', 'Paquetes', 'View_Ventas', 'View_Deacs', 'View_Ventas_SSAA']:
            self.parameters['dboperation'] = 'read_complex'
            ending = ['activacion' if section in ['Gross_Comision', 'Paquetes', 'View_Ventas', 'View_Ventas_SSAA'] else 'desactivacion']
            keyperiod = 'periodo_' + ending[0]
            self.parameters['keyperiod'] = keyperiod
        else:
            self.parameters['dboperation'] = 'read'
        
        querys = self.sqlmaker(self.parameters)
        self.parameters['sql'] = querys['sql']
        self.parameters['sqldel'] = querys['sqldel']
        
        dbobj = DbSqLiteOperator(self.parameters)
        dbobj.openDb()
        df = dbobj.readTbl(self.parameters['sql'])
        dbobj.closeDb()
        df = df.fillna('')
        
        df.name = section
        comment = ''
        
        paramstable = {'section' : self.section, 'lenght' : len(df), 'comment' : comment}
        self.display(paramstable)
        
        return df
        
    def dbOperation(self, operation, section, data = None):
            
        self.section = section
        self.configParameters()
        self.parameters['dboperation'] = operation       
        
        if operation == 'insert':
            dataprepare = dp.PlainDataFrame(self.parameters)      
            keyperiod, df = dataprepare.prepareCols(data)
            self.parameters['cols'] = df.columns.tolist()
            self.parameters['keyperiod'] = keyperiod
        elif operation == 'update':
            if section == 'Bolsas':
                computebol = dc.ComputeBolsas()
                df = computebol.prepareDf(data)
                
            elif section == 'SumVentaSSAA':
                compute = dc.ComputeSumSSAA()
                df = compute.prepareDf(data)
                
            elif section == 'Paquetes':             
                data = self.loadData('Paquetes')
                self.parameters['dboperation'] = operation # retomando el proceso update
                computepaq = dc.ComputePaquetes()
                df = computepaq.prepareDf(data)

            elif section == 'Gross_Comision':
                rules = self.loadData('tblGrossRules')
                rules = rules[rules['STATE_RULE'] != 'not_active']            
                rules.drop(self.parameters['dropcols'], axis = 1, inplace =True)
                #rules.to_csv('D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/' + 'gross_rules.csv')
                
                data = self.loadData('Gross_Comision')
                self.parameters['dboperation'] = operation # retomando el proceso update
                computegross = dc.ComputeGrossComision(self.parameters, rules)
 
                df = computegross.prepareDf(data)
            
            elif section ==  'Reversiones':
                rules = self.loadData('tblReversionesRules')      
                rules = rules[rules['STATE_RULE'] != 'not_active']
                rules.drop(self.parameters['dropcols'], axis = 1, inplace =True)
                
                data = self.loadData('Reversiones')
                self.parameters['dboperation'] = operation # retomando el proceso update
                computerev = dc.ComputeReversiones(self.parameters, rules)
                
                df = computerev.prepareDf(data)
                
            elif section == 'Unitarios':
                df = data[data['COMISION_UNITARIA'] != 0]
                df.reset_index(inplace = True,drop = True)
                
            else:
                df = data.copy()
            
            self.parameters['cols'] = self.parameters['criterycols'] + self.parameters['colstoupdate'] 
               
        querys = self.sqlmaker(self.parameters)
        dbobj = DbSqLiteOperator(self.parameters)
        dbobj.openDb()
        
        if operation == 'insert':
            comment = 'insertados'
             # Generando los argumentos a insertar
            tuplas = [tuple(x) for x in df.values] 
               
            dbobj.deleteTbl(querys['sqldel'])
            dbobj.writeTbl(querys['sql'], tuplas)
            
        elif operation == 'update':
            comment = 'actualizados'
            df[self.parameters['cols']].to_sql(self.parameters['tblname'] + '_temp', dbobj.conn, if_exists = 'replace', index = False)
            dbobj.updateTbl(querys['sql'])
            
        dbobj.closeDb()       
        
        paramstable = {'section' : self.section, 'lenght' : len(df), 'comment' : comment}
        self.display(paramstable)

    def downLoadTable(self, operation, section):
          
        self.section = section
        self.configParameters()
        self.parameters['dboperation'] = operation
        querys = self.sqlmaker(self.parameters)
        
        dbobj = DbSqLiteOperator(self.parameters)
        dbobj.openDb()
        df = dbobj.readTbl(querys)
        dbobj.closeDb()
        
        return df
        
    def sqlmaker(self, parameters):
        
        """ En insert construye una sentencia SQL. Las columnas no pueden estar con espacios.      
        sql = 'INSERT INTO ' + tblhis_ventas + ' (RAZON_SOCIAL,CONTRATO,FECHA_PROCESO) VALUES (?, ?, ?)' """
        
        """
        En update se crea una tabla temporal(tbl_source) y las columnas se actualizan en la tabla destino (tbl_outcome)
        """
        tblname = parameters['tblname']
        sqldel = ''
        
        if parameters['dboperation'] == 'insert':
            sql = 'INSERT INTO ' + tblname + ' (' + ', '.join(col for col in parameters['cols']) + ')' + \
            ' VALUES ' + '(' + ', '.join('?' for col in parameters['cols']) +')'
            
            sqldel = 'DELETE FROM ' + tblname + ' WHERE ' + tblname + '.' + self.parameters['keyperiod'] + ' = ' + self.month
            
        elif parameters['dboperation'] == 'update':       
            sql = 'UPDATE ' + tblname + ' SET ' + \
            ', '.join(col + ' = ' + '(SELECT ' + col + ' FROM ' + tblname + '_temp' +' WHERE ' + 
                      ' AND '.join(tblname + '_temp' + '.' + col2 + ' = ' + tblname + '.' + col2 for col2 
                                  in parameters['criterycols']) + ')' for col in parameters['cols']) + \
            ' WHERE ' + 'AND '.join(col2 + ' IN(SELECT ' + col2 + ' FROM ' + tblname + '_temp' + ')' for col2 in
                                    parameters['criterycols'])
                                                                        
        elif parameters['dboperation'] == 'read':
            sql = 'SELECT * FROM ' + tblname
            
        elif parameters['dboperation'] == 'read_complex':
            sql = 'SELECT * FROM ' + parameters['view'] + ' WHERE ' + parameters['keyperiod'] + ' = ' + self.month
        
        querys = {'sql': sql, 'sqldel' : sqldel}
        
        return querys    
    
    def setParser(self, parser):
        self.parser = parser