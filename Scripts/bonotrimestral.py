import logging

from Loader import fileloader as fl
from Loader import dfutils
import xlwings as xw
import pandas as pd
import numpy as np

'''
Bono Trimestral
 1. Leemos 2 archivos, comisiones de los ultimos 2 meses. Leemos las columnas de Interes.
 2. Escribimos lo leido en columnas del mismo nombre en el archivo excel del Bono
 3. Jalamos las formulas
'''

#Cargamos el logger
logger = logging.getLogger("")
logger.setLevel(logging.INFO)

month = '201901'

inifile = fl.ReadIniFile(mercado="personas")
defaultpath = inifile.getDataPath()
testpath = inifile.getTestPath()
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

#abrir archivo de comisionantes


#crear un dataframe con las columnas
