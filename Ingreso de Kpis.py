from Loader import fileloader as fl
from Loader import datahandledatabase as dhdb
from Loader import datacompute as dc
from Loader import datapreparation as dp
import pandas as pd
import numpy as np
from datetime import datetime
import time
import posixpath

month = '201812'
month_unitario = 201812

inifile = fl.ReadIniFile()

defaultpath = inifile.getDefaultPath()
testpath = inifile.getTestPath()

# Configurando los archivos de configuración para importar archivos tipo : xlsx, txt, ini
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)


logins = loader.loadFile('Logins')
metricasconjuntas = loader.loadFile('Metricas_conjuntas')
kpispltfrs = loader.loadFile('Kpis_plataformas')
comisionantespltfrs = loader.loadFile('Comisionantes_plataformas')
panelpltfrs0 = loader.loadHistoricalFile('Panel_plataformas')
periodo = loader.getPeriodo()

paramspltfrs= {'tipo' : 'plataformas', 'periodo' : periodo.upper(), 'keycol': 'NOMBRES', 'logins' : logins,
               'metricasconjuntas' : metricasconjuntas, 'kpis' : kpispltfrs, 'comisionantes' : comisionantespltfrs,
               'cuotas' : panelpltfrs0, 'resultados' : None}

panelpltfrs = loader.prepareHistoricalFiles(paramspltfrs)

#panelpltfrs.to_csv(posixpath.join(testpath, month + '_panelpltfrs.csv'),