"""
Script que lee la plantilla de reporte donde cada fila representa un reporte a ser creado.
Entonces cada fila se representa por un objeto que es instanciado de acuerdo a las propiedades de un reporte.
"""
import logging
import ecomis
import pandas as pd
import numpy as np
import posixpath
import datetime
import time
import sys
import xlwings as xw
import win32com.client as win32com
from win32com.client import constants

"""Clase que representa una fila del archivo de configuracion de reportes.
scope: nombre que identifica el equipo, gerencia, zona, etc a ser filtrado
id_file_origin: id que especifica el archivo de donde tomar la data
dest_file: nombre de archivo de destino del reporte
col_ref_num: numero de columna en los archivos de origen (iniciando por izquierda) donde se filtra segun 'scope'
"""

class ReportConfigItem(object):
  def __init__(self, scope, id_file_origin, dest_file, scope_column_id):
    self.scope = scope
    self.id_file_origin =  int(id_file_origin)
    self.dest_file = dest_file
    self.scope_column_id = int(scope_column_id)
    self.comis_file_item = None
  def getFileId(self):
    return self.id_file_origin
  def getDestFile(self):
    return self.dest_file
  def setComisFileItem(self, item):
    self.comis_file_item = item
  def getComisFileItem(self):
    return self.comis_file_item
  def getScopeColumnId(self):
    return self.scope_column_id
  def getScope(self):
    return self.scope

#Class that represent the file container
class ReportFileContainer(object):
  def __init__(self):
    self.sheets = {}
  def addSheet(self, sheet):
    self.sheets.append(sheet)
  def getReportSheetByFileId(self, fileid):
    for i in self.sheets:
      if i.getFileID() == fileid:
        return i

"""Clase que abre el archivo de configuracion de reportes.
   Posee un diccionario donde se agregan los ReportConfigItem por cada fila leida.
   
   inifile: Como vamos a utilizar 'ecomis' necesitamos la configuracion parseada en esta variable.
   
"""
class ReportConfigFile(object):
  def __init__(self, inifile, comis_file_collection):
    self.inifile = inifile
    self.section = ecomis.SectionObj(self.inifile, 'ReporteConfig')
    self.loader = ecomis.LoadFileProcess(self.section)
    self.comis_file_collection = comis_file_collection
    self.datadir = self.section.getParameter("datadir")[0]
    self.report_items = []
  def loadDf(self):
    self.df = self.loader.loadFile()
    self.df = self.df[self.df['GENFLAG']==1]
  def populateItems(self):
    self.loadDf()
    for index, row in self.df.iterrows():
      scope_ = row["SCOPE"]
      fileid_ = int(row["FILEID"])
      destfile_ = posixpath.join(self.datadir,row["NOMBRE_ARCHIVO_DESTINO"])
      scope_col_id_ = int(row["SCOPE_COLUMN_ID"])
      report_item = ReportConfigItem(scope_, fileid_, destfile_, scope_col_id_)
      comis_file_ = self.comis_file_collection.getItemById(fileid_)
      report_item.setComisFileItem(comis_file_)
      self.report_items.append(report_item)
  def getIdsOfUsedComisFiles(self):
    tmp_df = self.df.drop_duplicates(subset="FILEID")
    file_ids = tmp_df["FILEID"].tolist()
    return file_ids
  def getItems(self):
    return self.report_items

"""Clase que representa el archivo de comisiones desde el cual se construira un reporte
"""
class comisionesFileItem(object):
  def __init__(self, id, section_name, inifile, period):
    self.period = period
    self.id = id
    self.section_name = section_name
    self.inifile = inifile
    self.filename = self.genFilename()
    self.file = None
    self.pandas_excel_file = None
    self.report_sheets = []
  def genFilename(self):
    section_ = ecomis.SectionObj(self.inifile, self.section_name, period)
    return (section_.getFileList())[0]
  def getFilename(self):
    return self.filename
  def getId(self):
    return self.id
  def addReportSheet(self, sheet):
    self.report_sheets.append(sheet)
  def getReportSheets(self):
    return self.report_sheets
  def setFile(self, f):
    self.file = f
  def setPandasExcelFile(self, f):
    self.pandas_excel_file = f
  def getPandasExcelFile(self):
    return self.pandas_excel_file
  def getXWFileLink(self):
    return self.file
  

class comisionesFileCollection(object):
  def __init__(self):
    self.items = {}
  def addItemById(self, id, item):
    self.items[id] = item
  def existItemById(self, id):
    if id in self.items.keys():
      return True
    else:
      return False
  def getItemById(self, id):
    if self.existItemById(id) :
      return self.items[id]
    else:
      sys.exit("Invalid dictionary key")
  def getItems(self):
    return self.items
  
  def getItemsValues(self):
    return self.items.values()
      

class sheetCell(object):
  def __init__(self, row, col):
    self.row = int(row)
    self.col = int(col)
  def r(self):
    return self.row
  def c(self):
    return self.col

"""Clase que representa una hoja y sus parametros
"""
class reportSheet(object):
  def __init__(self, name, header_left_start, header_right_end, data_start):
    self.name = name
    self.header_left_start = header_left_start
    self.header_right_end = header_right_end
    self.data_start = data_start
  def getHeaderStartCell_T(self):
    return (self.header_left_start.r(), self.header_left_start.c())
  def getHeaderEndCell_T(self):
    return (self.header_right_end.r(), self.header_right_end.c())
  def getHeaderEndCell(self):
    return self.header_right_end
  def getDataStartCell_T(self):
    return (self.data_start.r(), self.data_start.c())
  def getDataStartCell(self):
    return self.data_start
  def getSheetName(self):
    return self.name



logger = logging.getLogger("juplogger")
handler = ecomis.LogViewver()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

# Variables globales
inifile = ecomis.ReadIniFile(mercado="empresas")
period = '201908'

# Configure the sheet objects
s1 = reportSheet("Comisionantes", sheetCell(1,1), sheetCell(3,140), sheetCell(3,1))
s2 = reportSheet("Activaciones", sheetCell(1,1), sheetCell(1,100), sheetCell(1,1))
s3 = reportSheet("Ajustes", sheetCell(1,1), sheetCell(1,10), sheetCell(1,1))
s4 = reportSheet("Reversiones", sheetCell(1,1), sheetCell(1,42), sheetCell(1,1))
s5 = reportSheet("Activaciones VAS", sheetCell(1,1), sheetCell(1,31), sheetCell(1,1))
s6 = reportSheet("VPN", sheetCell(1,1), sheetCell(1,45), sheetCell(1,1))

# Configuramos los objetos que representan los archivos de comisiones
comisiones_files_collection = comisionesFileCollection()
cfi1 = comisionesFileItem(1, 'Comisionantes_GrandesCuentas_All', inifile, period)
cfi1.addReportSheet(s1)
cfi1.addReportSheet(s2)
cfi1.addReportSheet(s3)
cfi1.addReportSheet(s4)
cfi1.addReportSheet(s5)
cfi1.addReportSheet(s6)
comisiones_files_collection.addItemById(1, cfi1)
cfi2 = comisionesFileItem(2, 'Comisionantes_Pymes_All', inifile, period)
cfi2.addReportSheet(s1)
cfi2.addReportSheet(s2)
cfi2.addReportSheet(s3)
cfi2.addReportSheet(s4)
cfi2.addReportSheet(s5)
cfi2.addReportSheet(s6)
comisiones_files_collection.addItemById(2, cfi2)
cfi3 = comisionesFileItem(3, 'Comisionantes_SolucionesNegocio_All', inifile, period)
cfi3.addReportSheet(s1)
cfi3.addReportSheet(s3)
comisiones_files_collection.addItemById(3, cfi3)
cfi4 = comisionesFileItem(4, 'Comisionantes_Plataformas_All', inifile, period)
cfi4.addReportSheet(s1)
cfi4.addReportSheet(s3)
comisiones_files_collection.addItemById(4, cfi4)


# Creamos la coleccion de objetos 'rc_items' que representan cada uno de los reportes a crearse
rc_file = ReportConfigFile(inifile, comisiones_files_collection)
rc_file.populateItems()
file_ids = rc_file.getIdsOfUsedComisFiles()
rc_items = rc_file.getItems()

# Abrimos los archivos que se van a necesitar para construir los reportes.
for item in comisiones_files_collection.getItemsValues():
  id_ = item.getId()
  if id_ in file_ids:
    fname_ = item.getFilename()
    item.setFile(xw.Book(fname_))
    item.setPandasExcelFile(pd.ExcelFile(fname_, encoding='utf-8'))

xl = win32com.gencache.EnsureDispatch('Excel.Application')

for item in rc_items:
  destfile_xw = xw.Book()
  cfi_ = item.getComisFileItem()
  for sheet in cfi_.getReportSheets():
    #Link to file objects
    comis_file_xw = cfi_.getXWFileLink()
    pandas_excelfile = cfi_.getPandasExcelFile()

    #Obteniendo punteros a hojas de libros.
    comis_sheet_xw = comis_file_xw.sheets(sheet.getSheetName())
    destfile_xw.sheets.add(name=sheet.getSheetName(), before="Sheet1")
    new_sheet_xw = destfile_xw.sheets(sheet.getSheetName())

    #Leyendo parametros de hojas
    origin_sheet_headers_range = comis_sheet_xw.range(sheet.getHeaderStartCell_T(), sheet.getHeaderEndCell_T())
    new_sheet_header_range = new_sheet_xw.range(sheet.getHeaderStartCell_T(), sheet.getHeaderEndCell_T())

    #Copiando cabeceras y pegando el formato
    origin_sheet_headers_range.api.Copy()
    new_sheet_header_range.api.PasteSpecial(Paste=constants.xlPasteFormats)
    new_sheet_header_range.value = origin_sheet_headers_range.value
    
    #last_data_row = comis_sheet_xw.api.Cells(65536, 1).End(xw.constants.Direction.xlUp).Row
    #last_data_col = comis_sheet_xw.api.Cells(sheet.getDataStartCell().r(), 10000).End(xw.constants.Direction.xlToLeft).Column
    
    # Old dataframe acquisicion
    #data_range = comis_sheet_xw.range( sheet.getDataStartCell_T(), (last_data_row, last_data_col) )
    #vvv = data_range.value
    #df = data_range.options(pd.DataFrame, index=False).value
    #df = pd.DataFrame(vvv)

    #Convirtiendo en dataframe la data con una funcion de pandas.
    df = pandas_excelfile.parse( sheet_name=sheet.getSheetName(), skiprows=sheet.getDataStartCell().r()-1, usecols=sheet.getHeaderEndCell().c()-1)
    #df = df.applymap(lambda x: str(x) if isinstance(x, datetime.time) else x)
    
    #Filtrando el dataframe de la data para que solo quede la gerencia, zonadeventa, etc de interes.
    df_cols_names = df.columns.values.tolist()
    scope_col_name = df_cols_names[item.getScopeColumnId()-1]
    scope_value = item.getScope()
    df = df[df[scope_col_name] == scope_value]
    #Escribiendo en la hoja del reporte
    new_sheet_xw.range( sheet.getDataStartCell().r()+1, sheet.getDataStartCell().c() ).options(index=False, header=None).value = df
    
    #Pegando formato en la data.
    range_format = comis_sheet_xw.range( ( sheet.getDataStartCell().r()+1, 1 ), ( sheet.getDataStartCell().r()+1 , sheet.getHeaderEndCell().c() ) )
    range_format.api.Copy()

    dest_range_format = new_sheet_xw.range( ( sheet.getDataStartCell().r() + 1, 1 ), ( len(df.index) + sheet.getDataStartCell().r(), sheet.getHeaderEndCell().c() ) )
    dest_range_format.api.PasteSpecial(Paste=constants.xlPasteFormats)
    
    print("s")

  destfile_xw.save(item.getDestFile())
  destfile_xw.close()

for item in comisiones_files_collection.getItemsValues():
  id_ = item.getId()
  if id_ in file_ids:
    wx_file_link = item.getXWFileLink()
    wx_file_link.close()


"""
#Class that represent the loaded comisiones file
class ReportFile(object):
  def __init__(self, name, sheetsname, month, fileid):
    
    self.name = name
    #fileid que figura en el archivo de reporteador plantilla
    self.fileid = fileid
    #Las paginas que se van a cargar como dataframes
    self.sheetsnames = sheetsname
    #El mes, sirve para saber el periodo del archivo de comisiones a cargar
    self.month = month
    #List of the loaded dataframes
    self.df = []

  def retrieveSheets(self):
    inifile = fl.ReadIniFile(mercado="empresas")
    section = fl.SectionObj(inifile, self.name, self.month)

    for sheetname in self.sheetsnames:
      section.setParameter('presetsheet', [sheetname])

      
      if sheetname == "Comisionantes":
        section.setParameter('skiprows', 2)
      else:
        section.setParameter('skiprows', 0)

      loader = fl.LoadFileProcess(section)
      df = loader.loadFile()
      self.df.append(df)

"""

"""
#Variable globales


month = '201908'

sections_names = {  '1':'Comisionantes_Plataformas_All',
                    '2':'Comisionantes_GrandesCuentas_All',
                    '3':'Comisionantes_SolucionesNegocio_All',
                    '4':'Comisionantes_Pymes_All'  }

inifile = fl.ReadIniFile(mercado="empresas")

sheetsnames = ['Comisionantes','Activaciones']
file1 = ReportFile("Comisionantes_Plataformas_All", sheetsnames, month,4)
file1.retrieveSheets()


file2 = ReportFile("Comisionantes_GrandesCuentas_All", sheetsnames, month,1)
file3 = ReportFile("Comisionantes_SolucionesNegocio_All", sheetsnames, month,3)
file4 = ReportFile("Comisionantes_Pymes_All", sheetsnames, month,2)


rfc = ReportFileContainer()
rfc.addSheet(file1)
rfc.addSheet(file2)
rfc.addSheet(file3)
rfc.addSheet(file4)


inifile = ecomis.ReadIniFile(mercado="empresas")

section_logins = ecomis.SectionObj(inifile,'Logins',month)
loader_logins = ecomis.LoadFileProcess(section_logins)
logins = loader_logins.loadFile()

#Container of the row objects
rows = []
#Leemos el archivo de generacion de reportes:
section_1 = fl.SectionObj(inifile,"Reporteador")
section_1.reloadParameters()
loader1 = fl.LoadFileProcess(section_1)
df_report = loader1.loadFile()

for row in df_report.head(79).itertuples():
    if row.IFFILEEXPORTED == 1:
      r_i_obj = ReportItem(row.SCOPE, row.FILEID, row.NOMBREGENERADO)
      rows.append(r_i_obj)
      r_i_obj.addReportSheet(rfc.getReportSheetByFileId(row.FILEID))

"""

#i=1 -> Comisiones Grandes Cuentas
#i=2 -> Comisiones Pymes
#i=3 -> Comisiones Soluciones de Negocio
#i=4 -> Comisiones Plataformas Comerciales
#Guardamos cada fila en un objeto que define la configuracion de un reporte.


"""
for i in rows:
  if i.getFileId() == 1:
    #leer dataframe de archivo tal
  elif i.getFileId() == 2:
  elif i.getFileId() == 3:
  elif i.getFileId() == 4:
  else:
    raise Exception('Fie id not recognized')


section_1 = fl.SectionObj(inifile,chosen_file,month)
section_1.setParameter('presetsheet','Leyenda')
loader1 = fl.LoadFileProcess(section_1)
pesospltfrs = loader1.loadFile()
"""