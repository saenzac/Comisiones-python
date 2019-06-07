"""
Script que lee la plantilla de reporte dond ecada fila representa un reporte a ser creado.
Entonces cada fila se representa por un objeto que es instanciado de acuerdo a las propiedades de un reporte.
"""

import logging
from Loader import fileloader_proto as fl
from Loader import datahandledatabase as dhdb
from Loader import datacompute as dc
from Loader import datapreparation as dp
import pandas as pd
import numpy as np
import posixpath
from datetime import datetime
import time
import sys


#Class that represents an row object for each row of the report template
class ReportItem(object):
  def __init__(self, scope, id_file_origin, dest_file):
    self.scope = scope
    self.id_file_origin =  id_file_origin
    self.dest_file = dest_file
    self.reportsheet = None

  def getFileId(self):
    return self.id_file_origin

  def addReportSheet(self, rs):
    self.reportsheet = rs

  def filterReportFile(self):
    return None

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
    section_1 = fl.SectionObj(inifile, self.name, self.month)

    for sheet in self.sheetsnames:
      section_1.setParameter('presetsheet', sheet)

      if sheet == "Activaciones" or sheet == "Activaciones VAS" or sheet == "Ajustes" or sheet == "Reversiones":
        section_1.setParameter('skiprows', 0)

      loader1 = fl.LoadFileProcess(section_1)
      df = loader1.loadFile()
      self.df.append(df)









#Variable globales
month = "201904"

comis_sections = {  '1':'Comisionantes_Plataformas_All',
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