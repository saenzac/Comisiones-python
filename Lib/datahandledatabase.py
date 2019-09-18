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
import posixpath
from pandas import Series, DataFrame
#from Loader import datapreparation as dp
#from Loader import datacompute as dc
import ecomis
import logging

logger = logging.getLogger("juplogger")

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
    def writeTbl(self, query):
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
        self.conn = sqlite3.connect(posixpath.join(self.dbpath,self.dbname), detect_types = sqlite3.PARSE_DECLTYPES)
        
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

    def customQueryTbl(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()
         
class DbDataProcess(object):
    
    def __init__(self, month):
        self.month = month
        self.parser = None
        self.section = None
        self.parameters = None
        self.dbpath = None

    def display(self, paramstable):
        logger.info('Los registros de la tabla %s es %s registros %s ' % ((paramstable['section'], paramstable['lenght'], paramstable['comment'])))

    """
       Crea una diccionario con los parametros de la seccion respectiva leidos del archivo de configuracion.
       En este caso el archivo de configuracion correspondiente a la base de datos: mydbconfig.ini
    """
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

    """
      Ejecucion de alguna operacion sql requerida como paso previo a la carga de una tabla  a un Dataframe.
    """
    def pre_loadData(self, section):
        self.section = section
        self.configParameters()
        if section in ['Reversiones']:
            #Crea una tabla temporal a partir de la vista llamada "Reversiones"
            #Esto debido a que 'Pandas' toma mucho tiempo en el procesamiento cuando se lee directamente de la vista.
            self.parameters['dboperation'] = 'create_temp_table_from_view'

        querys = self.sqlmaker(self.parameters)

        #Guardamos las setencias sql en el diccionario de parametros para su posible uso posterior.
        self.parameters['sql'] = querys['sql']
        self.parameters['sqldel'] = querys['sqldel']

        #Ejecutamos las setencias definidas en self.parameters['sqldel'] y self.parameters['sql']
        dbobj = DbSqLiteOperator(self.parameters)
        dbobj.openDb()
        dbobj.customQueryTbl(self.parameters['sqldel'])
        dbobj.customQueryTbl(self.parameters['sql'])
        dbobj.closeDb()

    """
      Carga data de una tabla hacia un dataframe
      Si la ejecucion previa de una sentencia sql es requerida usar primero la funcion self.pre_loadData()
    """
    def loadData(self, section):
        self.section = section
        self.configParameters()
        if section in ['Gross_Comision', 'Paquetes','View_VAS_Voz','View_Deacs_SSAA']:
            self.parameters['dboperation'] = 'read_complex'
            ending = ['activacion' if section in ['Gross_Comision', 'Paquetes','View_VAS_Voz'] else 'desactivacion']
            keyperiod = 'periodo_' + ending[0]
            self.parameters['keyperiod'] = keyperiod
        # El siguiente bloque se elimina una ves que se tome la nueva estructura de obtener la información se usa read_more_periods           
        elif section in ['View_Ventas', 'View_Inar_Tiendas_Propias_Blanks', 'View_Ventas_SSAA', 'View_Deacs', 'View_Test']:
            self.parameters['dboperation'] = 'read_more_periods'
            ending = ['activacion' if section in ['View_Ventas', 'View_Inar_Tiendas_Propias_Blanks', 'View_Ventas_SSAA', 'View_Test'] else 'desactivacion']
            keyperiod = 'periodo_' + ending[0]
            self.parameters['keyperiod'] = keyperiod
        elif section in ['Reversiones']:
            #Leemos de la tabla temporal creada en la funcion pre_loadData()
            self.parameters['dboperation'] = 'read_complex_temp_table'
            ending = ['desactivacion']
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

        if operation == 'insert' or operation == 'insertwodeletion':
            dataprepare = ecomis.PlainDataFrame(self.parameters)
            keyperiod, df = dataprepare.prepareCols(data)
            self.parameters['cols'] = df.columns.tolist()
            self.parameters['keyperiod'] = keyperiod
        elif operation == 'update':
            if section == 'Bolsas':
                computebol = ecomis.ComputeBolsas()
                df = computebol.prepareDf(data)

            elif section == 'SumVentaSSAA':
                compute = ecomis.ComputeSumSSAA()
                df = compute.prepareDf(data)

            elif section == 'Paquetes':             
                data = self.loadData('Paquetes')
                self.parameters['dboperation'] = operation # retomando el proceso update
                computepaq = ecomis.ComputePaquetes()
                df = computepaq.prepareDf(data)

            elif section == 'Gross_Comision':
                rules = self.loadData('tblGrossRules')
                rules = rules[rules['STATE_RULE'] != 'not_active']            
                rules.drop(self.parameters['dropcols'], axis = 1, inplace =True)
                #rules.to_csv('D:/Datos de Usuario/cleon/Documents/Capital Humano/Data Fuente Comisiones/test/' + 'gross_rules.csv')
                
                data = self.loadData('Gross_Comision')
                self.parameters['dboperation'] = operation # retomando el proceso update
                computegross = ecomis.ComputeGrossComision(self.parameters, rules)
 
                df = computegross.prepareDf(data)

            elif section ==  'Reversiones':
                rules = self.loadData('tblReversionesRules')      
                rules = rules[rules['STATE_RULE'] != 'inactive']
                rules.drop(self.parameters['dropcols'], axis = 1, inplace =True)

                #Copiamos la vista Reversiones a una tabla temporal para leer la data desde dicho origen.
                #Cuando se lee directamente de la vista el performance es muy pobre (demora casi 1 hora)
                self.pre_loadData('Reversiones')
                #dataframe de la tabla temporal creada en pre_loadData()
                data = self.loadData('Reversiones')
                self.parameters['dboperation'] = operation # retomando el proceso update


                computerev = ecomis.ComputeReversiones(self.parameters, rules)
                df = computerev.prepareDf(data)
            elif section == 'Unitarios':
                df = data[data['COMISION_UNITARIA'] != 0]                
                df = df.drop_duplicates(['CONTRATO'], keep='last')
                df.reset_index(inplace = True,drop = True)
                #print(len(df))# control

            else:
                df = data.copy()
                #print(df.columns)

            self.parameters['cols'] = self.parameters['criterycols'] + self.parameters['colstoupdate'] 

        #print(self.parameters['cols']) # punto de test
        querys = self.sqlmaker(self.parameters)
        dbobj = DbSqLiteOperator(self.parameters)
        dbobj.openDb()
        
        if operation == 'insert':
            comment = 'insertados'
             # Generando los argumentos a insertar
            tuplas = [tuple(x) for x in df.values] 
               
            dbobj.deleteTbl(querys['sqldel'])
            dbobj.writeTbl(querys['sql'], tuplas)
        elif operation == 'insertwodeletion':
            comment = 'insertados'
             # Generando los argumentos a insertar
            tuplas = [tuple(x) for x in df.values]
            dbobj.writeTbl(querys['sql'], tuplas)
        elif operation == 'update':
            comment = 'actualizados'
            df[self.parameters['cols']].to_sql(self.parameters['tblname'] + '_temp', dbobj.conn, if_exists='replace', index=False)
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
        """
        Genera sentencias sql de acuerdo a la operación deseada

        ** En "insert" construye una sentencia SQL. Las columnas no pueden estar con espacios.
        sql = 'INSERT INTO ' + tblhis_ventas + ' (col[0],col[1],col[2]) VALUES (?, ?, ?)'

        ** En "update" actualiza una tabla insertando primero la data en una tabla temporal.
          Se crea una tabla temporal y las columnas se actualizan en la tabla destino.
            Ejemplo:
            tblname = "tblname"
            parameters = {}
            parameters['cols'] = ['COLS1', 'COLS2']
            parameters['criterycols'] = ['CRITERYCOL1', 'CRITERYCOL2','CRITERYCOL3']

            Resulting sql sentence >>>
            UPDATE tblname SET
            COLS1 = (SELECT COLS1 FROM tblname_temp WHERE tblname_temp.CRITERYCOL1 = tblname.CRITERYCOL1 AND tblname_temp.CRITERYCOL2 = tblname.CRITERYCOL2 AND tblname_temp.CRITERYCOL3 = tblname.CRITERYCOL3),
            COLS2 = (SELECT COLS2 FROM tblname_temp WHERE tblname_temp.CRITERYCOL1 = tblname.CRITERYCOL1 AND tblname_temp.CRITERYCOL2 = tblname.CRITERYCOL2 AND tblname_temp.CRITERYCOL3 = tblname.CRITERYCOL3)
            WHERE
            CRITERYCOL1 IN(SELECT CRITERYCOL1 FROM tblname_temp) AND
            CRITERYCOL2 IN(SELECT CRITERYCOL2 FROM tblname_temp) AND
            CRITERYCOL3 IN(SELECT CRITERYCOL3 FROM tblname_temp)
            <<<
        """

        tblname = parameters['tblname']
        sqldel = ''

        if parameters['dboperation'] == 'insert':
            sql = 'INSERT INTO ' + tblname + ' (' + ', '.join(col for col in parameters['cols']) + ')' + \
            ' VALUES ' + '(' + ', '.join('?' for col in parameters['cols']) +')'

            sqldel = 'DELETE FROM ' + tblname + ' WHERE ' + tblname + '.' + self.parameters['keyperiod'] + ' = ' + self.month

        elif parameters['dboperation'] == 'insertwodeletion':
            sql = 'INSERT INTO ' + tblname + ' (' + ', '.join(col for col in parameters['cols']) + ')' + \
            ' VALUES ' + '(' + ', '.join('?' for col in parameters['cols']) +')'
            sqldel = ''

        elif parameters['dboperation'] == 'update':
            sql = 'UPDATE ' + tblname + ' SET ' + \
            ', '.join(col + ' = ' + '(SELECT ' + col + ' FROM ' + tblname + '_temp' +' WHERE ' + 
                      ' AND '.join(tblname + '_temp' + '.' + col2 + ' = ' + tblname + '.' + col2 for col2 
                                  in parameters['criterycols']) + ')' for col in parameters['cols']) + \
            ' WHERE ' + 'AND '.join(col2 + ' IN(SELECT ' + col2 + ' FROM ' + tblname + '_temp' + ')' for col2 in
                                    parameters['criterycols'])

        #Read from table, no view.
        elif parameters['dboperation'] == 'read':
            sql = 'SELECT * FROM ' + tblname

        #Read from view taking into account the activation or deactivation period
        elif parameters['dboperation'] == 'read_complex':
            sql = 'SELECT * FROM ' + parameters['view'] + ' WHERE ' + parameters['keyperiod'] + ' = ' + self.month

        elif parameters['dboperation'] == 'read_more_periods':
            sql = 'SELECT * FROM ' + parameters['view'] + ' WHERE ' + parameters['keyperiod'] + ' >= ' + self.month

        #Delete temporal table and copy view to that temporal table.
        elif parameters['dboperation'] == 'create_temp_table_from_view':
            sql = 'CREATE TABLE ' +  tblname + '_temp' +  ' AS SELECT * FROM ' + parameters['view'];
            sqldel = 'DROP TABLE ' + tblname + '_temp'

        #Read from temporal table
        elif parameters['dboperation'] == 'read_complex_temp_table':
            sql = 'SELECT * FROM ' + tblname + '_temp' + ' WHERE ' + parameters['keyperiod'] + ' = ' + self.month

        querys = {'sql': sql, 'sqldel' : sqldel}

        return querys    

    def setParser(self, parser):
        self.parser = parser
        dbpath = parser['DEFAULT']['databasepath']
        if self.parser['DEFAULT']['mercado'] == "empresas":
            self.dbname = 'mercado_empresas_db.sqlite'
        elif self.parser['DEFAULT']['mercado'] == "personas":
            self.dbname = 'mercado_personas_db.sqlite'
        else:
            logger.error("Bad argument selection")
            raise Exception("Neither mercado 'empresas' or 'personas' selected")
            sys.exit(1)

        #dbpath = posixpath.join(parser['DEFAULT']['databasepath'],'Bases')
        self.setDbPath(dbpath)
        logger.info('Setting database path to ' + posixpath.join(self.dbpath, self.dbname))

    def setDbPath(self,dbpath):
        self.dbpath = dbpath

    def setDbName(self,dbname):
        self.dbname = dbname