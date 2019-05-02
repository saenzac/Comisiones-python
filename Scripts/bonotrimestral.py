import logging

from Loader import fileloader as fl
from Loader import dfutils
import xlwings as xw
import pandas as pd
import numpy as np

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

