from Loader import fileloader as fl
from Loader import datahandledatabase as dhdb
from Loader import datacompute as dc
from Loader import datapreparation as dp
import xlwings as xw
import pandas as pd
import numpy as np
from datetime import datetime
import time
import posixpath

month = '201812'

inifile = fl.ReadIniFile()
defaultpath = inifile.getDefaultPath()
testpath = inifile.getTestPath()
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

"""
comisionantespltfrs = loader.loadFile('Comisionantes_plataformas_rproductividad')
#Obteniendo indice de la columna de porcentaje ponderado
ponderado_cindex = comisionantespltfrs.columns.get_loc("PORCENTAJE_TOTAL_PONDERADO") + 1
#Abriendo archivo y haciendo hola 'comisionantes' activa
file = loader.getFileList()[0]
wb = xw.Book(file)
mysheet = wb.sheets('Comisionantes')
#Inmovilizando valores de porcentajes ponderados
lastrow = mysheet.api.Cells(65536,ponderado_cindex).End(xw.constants.Direction.xlUp).Row
mysheet.range((1,ponderado_cindex)).options(transpose=True).value = mysheet.range((1,ponderado_cindex),(lastrow,ponderado_cindex)).value
"""

pesospltfrs = loader.loadFile('Pesos_plataformas')
pesosdf = pesospltfrs
pesosdf = pesosdf[pesosdf['ITEM'] != 'ITEM']
pesosdf = pesosdf[~pesosdf['ITEM'].isnull()]
pesosdf = pesosdf[pesosdf['ITEM'] < 3000]
pesosdf = pesosdf[pesosdf['ITEM'] > 2000]

capturadf=pesospltfrs(regex=("CAPTURA.*"))
gestiondf=pesospltfrs(regex=("GESTIÓN.*"))
desarrollodf=pesospltfrs(regex=("DESARROLLO.*"))

a=12