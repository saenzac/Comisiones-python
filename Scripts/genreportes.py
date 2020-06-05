# -*- coding: utf-8 -*-

"""
Script que lee la plantilla de reporte donde cada fila representa un reporte a ser creado.
Entonces cada fila se representa por un objeto que es instanciado de acuerdo a las propiedades de un reporte.
s
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
  def __init__(self, scope, id_file_origin, dest_file, scope_column_id, canal_leyenda):
    self.scope = scope
    self.id_file_origin =  int(id_file_origin)
    self.dest_file = dest_file
    self.scope_column_id = int(scope_column_id)
    self.comis_file_item = None
    self.canal_leyenda = canal_leyenda
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
  def getCanalLeyenda(self):
    return self.canal_leyenda 
  def getDestFileBasename(self):
    return posixpath.basename(self.dest_file)

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
  def __init__(self, inifile, comis_file_collection, periodo):
    self.inifile = inifile
    self.section = ecomis.SectionObj(self.inifile, 'ReporteConfig')
    self.loader = ecomis.LoadFileProcess(self.section)
    self.comis_file_collection = comis_file_collection
    self.datadir = self.section.getParameter("datadir")[0]
    self.report_items = []
    self.df = None
    self.periodo = periodo
  def loadDf(self):
    self.df = self.loader.loadFile()
    self.df = self.df[self.df['GENFLAG']==1]
  def populateItems(self):
    self.loadDf()
    for index, row in self.df.iterrows():
      scope_ = row["SCOPE"]
      fileid_ = int(row["FILEID"])
      destfile_ = posixpath.join(self.datadir, self.periodo + "_" + row["NOMBRE_ARCHIVO_DESTINO"])
      scope_col_id_ = int(row["SCOPE_COLUMN_ID"])
      canal_leyenda = row["CANAL_LEYENDA"]
      report_item = ReportConfigItem(scope_, fileid_, destfile_, scope_col_id_,canal_leyenda)
      comis_file_ = self.comis_file_collection.getItemById(fileid_)
      report_item.setComisFileItem(comis_file_)
      self.report_items.append(report_item)
  def getIdsOfUsedComisFiles(self):
    tmp_df = self.df.drop_duplicates(subset="FILEID")
    file_ids = tmp_df["FILEID"].tolist()
    return file_ids
  def getItems(self):
    return self.report_items
  def getLenItems(self):
    return len(self.report_items)

"""Clase que representa el archivo de comisiones desde el cual se construira un reporte
"""
class comisionesFileItem(object):
  def __init__(self, id, section_name, inifile, period):
    self.period = period
    self.id = id
    self.section_name = section_name
    self.inifile = inifile
    self.filename = self.genFilename()
    self.xw_file_link = None
    self.pandas_excel_file = None
    self.report_sheets = []
    self.dfs = {}
  def addDFSheetByName(self, df, name):
    self.dfs[name] = df
  def getDFSheetByName(self, name):
    return self.dfs[name]
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
  def setXWFileLink(self, f):
    self.xw_file_link = f
  def setPandasExcelFile(self, f):
    self.pandas_excel_file = f
  def getPandasExcelFile(self):
    return self.pandas_excel_file
  def getXWFileLink(self):
    return self.xw_file_link
  def getSectionName(self):
    return self.section_name
  

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
  def getName(self):
    return self.name



logger = logging.getLogger("juplogger")
#handler = ecomis.LogViewver()
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


# Variables globales
inifile = ecomis.ReadIniFile(mercado="empresas")
period = '202004'

# Configure the sheet objects
s1 = reportSheet("Comisionantes", sheetCell(1,1), sheetCell(3,165), sheetCell(4,1))
s2 = reportSheet("Activaciones", sheetCell(1,1), sheetCell(1,100), sheetCell(2,1))
s3 = reportSheet("Ajustes", sheetCell(1,1), sheetCell(1,25), sheetCell(2,1))
s4 = reportSheet("Reversiones", sheetCell(1,1), sheetCell(1,46), sheetCell(2,1))
s5 = reportSheet("Activaciones VAS", sheetCell(1,1), sheetCell(1,31), sheetCell(2,1))
s6 = reportSheet("VPN", sheetCell(1,1), sheetCell(1,45), sheetCell(2,1))
s7 = reportSheet("Leyenda", sheetCell(1,1), sheetCell(3,22), sheetCell(4,1))
s8 = reportSheet("M2M", sheetCell(1,1), sheetCell(1,19), sheetCell(2,1))
s9 = reportSheet("Activaciones", sheetCell(1,1), sheetCell(1,32), sheetCell(2,1))
s10 = reportSheet("Desactivaciones", sheetCell(1,1), sheetCell(1,55), sheetCell(2,1))
#s11 = reportSheet("BaseInicio", sheetCell(1,1), sheetCell(1,30), sheetCell(2,1))
s12 = reportSheet("CuotasVasdeVOZ", sheetCell(1,1), sheetCell(1,17), sheetCell(2,1))
s13 = reportSheet("Resultados", sheetCell(1,1), sheetCell(1,17), sheetCell(2,1))
s14 = reportSheet("Churn", sheetCell(1,1), sheetCell(1,9), sheetCell(2,1))
s15 = reportSheet("Promedio6", sheetCell(1,1), sheetCell(1,17), sheetCell(2,1))

# Configuramos los objetos que representan los archivos de comisioness1
comisiones_files_collection = comisionesFileCollection()

cfi2 = comisionesFileItem(2, 'Comisionantes_Pymes_All', inifile, period)
""""
cfi2.addReportSheet(s1)
cfi2.addReportSheet(s2)
cfi2.addReportSheet(s3)
cfi2.addReportSheet(s4)
cfi2.addReportSheet(s5)
cfi2.addReportSheet(s6)
cfi2.addReportSheet(s7)
"""
cfi2.addReportSheet(s1)
cfi2.addReportSheet(s3)
cfi2.addReportSheet(s15)
comisiones_files_collection.addItemById(2, cfi2)

cfi1 = comisionesFileItem(1, 'Comisionantes_VentaRegionaEmpresa_All', inifile, period)
""""
cfi1.addReportSheet(s1)
cfi1.addReportSheet(s2)
cfi1.addReportSheet(s3)
cfi1.addReportSheet(s4)
cfi1.addReportSheet(s5)
cfi1.addReportSheet(s6)
cfi1.addReportSheet(s7)
"""
cfi1.addReportSheet(s1)
cfi1.addReportSheet(s3)
cfi1.addReportSheet(s15)
comisiones_files_collection.addItemById(1, cfi1)


"""
cfi3 = comisionesFileItem(3, 'Comisionantes_SolucionesNegocio_All', inifile, period)
cfi3.addReportSheet(s1)
#cfi3.addReportSheet(s3)
#cfi3.addReportSheet(s7)
cfi3.addReportSheet(s8)
cfi3.addReportSheet(s9)
cfi3.addReportSheet(s10)
cfi3.addReportSheet(s12)
cfi3.addReportSheet(s13)
comisiones_files_collection.addItemById(3, cfi3)
"""

cfi4 = comisionesFileItem(4, 'Comisionantes_Plataformas_All', inifile, period)
cfi4.addReportSheet(s1)
cfi4.addReportSheet(s3)
cfi4.addReportSheet(s7)


comisiones_files_collection.addItemById(4, cfi4)

cfi5 = comisionesFileItem(5, 'Comisionantes_Corporaciones_All', inifile, period)
"""
fi5.addReportSheet(s1)
cfi5.addReportSheet(s2)
cfi5.addReportSheet(s3)
cfi5.addReportSheet(s4)
cfi5.addReportSheet(s5)
cfi5.addReportSheet(s6)
cfi5.addReportSheet(s7)
"""
cfi5.addReportSheet(s1)
cfi5.addReportSheet(s3)
cfi5.addReportSheet(s15)
comisiones_files_collection.addItemById(5, cfi5)

cfi6 = comisionesFileItem(6, 'Comisionantes_GC_DDNN_IS_All', inifile, period)
"""
cfi6.addReportSheet(s1)
cfi6.addReportSheet(s2)
cfi6.addReportSheet(s3)
cfi6.addReportSheet(s4)
cfi6.addReportSheet(s5)
cfi6.addReportSheet(s6)
cfi6.addReportSheet(s7)
#cfi6.addReportSheet(s14)
"""
cfi6.addReportSheet(s1)
cfi6.addReportSheet(s3)
cfi6.addReportSheet(s15)
comisiones_files_collection.addItemById(6, cfi6)


"""
cfi7 = comisionesFileItem(7, 'Comisionantes_GC_Antiguo_All', inifile, period)
cfi7.addReportSheet(s1)
cfi7.addReportSheet(s2)
cfi7.addReportSheet(s5)
cfi7.addReportSheet(s7)
comisiones_files_collection.addItemById(7, cfi7)
"""

# Creamos la coleccion de objetos 'rc_items' que representan cada uno de los reportes a crearse
rc_file = ReportConfigFile(inifile, comisiones_files_collection, period)
rc_file.populateItems()
used_files_ids = rc_file.getIdsOfUsedComisFiles()
rc_items = rc_file.getItems()

# Abrimos los archivos que se van a necesitar para construir los reportes.
for comis_file in comisiones_files_collection.getItemsValues():
  id_ = comis_file.getId()
  if id_ in used_files_ids:
    logger.info("Procesando sección: " + comis_file.getSectionName())
    fname_ = comis_file.getFilename()
    logger.info("Abriendo archivo asociado a la sección")
    comis_file.setXWFileLink(xw.Book(fname_))
    logger.info("Guardando objeto <Pandas>.ExcelFile")
    comis_file.setPandasExcelFile(pd.ExcelFile(fname_))
    
    # Aca debemos de interar por las paginas del libro y comenzar a cargar en variables que almacenen los dataframes
    # Pero tambient enemos que usar los parametros configurados : Objeto reportSheet
    for rsheet in comis_file.getReportSheets():
      excel_file = comis_file.getPandasExcelFile()
      excel_file = comis_file.getPandasExcelFile()
      logger.info("Generando y guardando dataframe " + "hoja: " + rsheet.getName())
      df = excel_file.parse( sheet_name=rsheet.getName(), skiprows=rsheet.getDataStartCell().r()-2, usecols=rsheet.getHeaderEndCell().c()-1)
      comis_file.addDFSheetByName(df, rsheet.getName())
      

xl = win32com.gencache.EnsureDispatch('Excel.Application')

#Procesamos todo
logger.info("Se van a procesar: " + str(rc_file.getLenItems()) + " reportes")
for item in rc_items:
  logger.info("Procesando reporte: " + item.getDestFileBasename())
  destfile_xw = xw.Book()
  cfi_ = item.getComisFileItem()
  for sheet in cfi_.getReportSheets():
    logger.info("  Hoja -> " + sheet.getName())
    #Link to file objects
    comis_file_xw = cfi_.getXWFileLink()

    #Obteniendo punteros a hojas de libros.
    comis_sheet_xw = comis_file_xw.sheets(sheet.getName())
    destfile_xw.sheets.add(name=sheet.getName(), before="Hoja1")
    new_sheet_xw = destfile_xw.sheets(sheet.getName())

    #Leyendo parametros de hojas
    origin_sheet_headers_range = comis_sheet_xw.range(sheet.getHeaderStartCell_T(), sheet.getHeaderEndCell_T())
    new_sheet_header_range = new_sheet_xw.range(sheet.getHeaderStartCell_T(), sheet.getHeaderEndCell_T())

    #Des-ocultando filas y columnas
    comis_sheet_xw.api.Columns.EntireColumn.Hidden = False
    comis_sheet_xw.api.Rows.EntireRow.Hidden = False
    #Copiando cabeceras
    origin_sheet_headers_range.api.Copy()
    new_sheet_header_range.api.PasteSpecial(Paste=constants.xlPasteFormats)    
    new_sheet_header_range.value = origin_sheet_headers_range.value
    
    #Referenciamos al dataframe ya cargado
    df = cfi_.getDFSheetByName(sheet.getName())

    #Filtrando el dataframe de la data para que solo quede la gerencia, zonadeventa, etc de interes.
    df_cols_names = df.columns.values.tolist()
    scope_col_name = df_cols_names[item.getScopeColumnId()-1]
    scope_value = item.getScope()
    scope_leyenda = item.getCanalLeyenda()

    if sheet.getName() == "Leyenda":
      if item.getScopeColumnId() in [4,5,6]:
        #Todos que no son gerentes de canal, para filtrar la hoja leyenda se usara el nombre de canal.
        scope = scope_leyenda
      else:
        scope = scope_value

      df = df[(df[scope_col_name] == scope) | df[scope_col_name].isnull() | (df[scope_col_name] == scope_col_name)]
    else:
      df = df[df[scope_col_name] == scope_value]

    #Escribiendo en la hoja del reporte
    new_sheet_xw.range( sheet.getDataStartCell().r(), sheet.getDataStartCell().c() ).options(index=False, header=None).value = df

    #Pegando formato de la primera fila de data en la demas data.
    range_format = comis_sheet_xw.range( ( sheet.getDataStartCell().r(), 1 ), ( sheet.getDataStartCell().r(), sheet.getHeaderEndCell().c() ) )
    range_format.api.Copy()

    dest_range_format = new_sheet_xw.range( ( sheet.getDataStartCell().r(), 1 ), ( len(df.index) + sheet.getDataStartCell().r() - 1, sheet.getHeaderEndCell().c() ) )
    dest_range_format.api.PasteSpecial(Paste=constants.xlPasteFormats)

    #Corrigiendo formato para el casod e hoja = Leyenda
    if sheet.getName() == "Leyenda":
      s_indices = list(np.where(df[scope_col_name] == scope_col_name)[0] + sheet.getDataStartCell().r() -2)
      for row in s_indices:
        origin_sheet_headers_range.api.Copy()
        range_subheaders = new_sheet_xw.range((int(row), 1))
        range_subheaders.api.PasteSpecial(Paste=constants.xlPasteFormats)    

  destfile_xw.save(item.getDestFile())
  destfile_xw.close()

#Cerramos todos los archivos abiertos
logger.info("Cerrando archivos abiertos.")
for item in comisiones_files_collection.getItemsValues():
  id_ = item.getId()
  if id_ in used_files_ids:
    wx_file_link = item.getXWFileLink()
    wx_file_link.close()
