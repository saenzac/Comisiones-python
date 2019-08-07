from Loader import fileloader as fl
from Loader import datahandledatabase as dhdb
from Loader import datacompute as dc
from Loader import datapreparation as dp
import pandas as pd
import numpy as np
from datetime import datetime
import time
import posixpath

month = '201906'

inifile = fl.ReadIniFile(mercado="empresas")

defaultpath = inifile.getDataPath()
testpath = inifile.getTestPath()

parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

logins = loader.loadFile('Logins') # ojo con tener logins duplicados. Sale errores raros
metricasconjuntas = loader.loadFile('Metricas_conjuntas')
kpisvoz = loader.loadFile('Kpis_voz_gc')
comisionantesvoz = loader.loadFile('Comisionantes_voz_gc')
cuotasvoz = loader.loadHistoricalFile('Cuotas')

periodo = loader.getPeriodo()

# Preparar los resultados antes de enviar el cerrado
# GCE_Resultados debe tener dos o mas Hojas sino lanza error

#resultadosvoz = loader.loadHistoricalFile('Resultados') # Por precaución eliminar tres columnas posteriores a dic-18.
resultadosvoz = pd.DataFrame() # Usar si no hay resultados

paramsvoz = {'tipo' : 'voz', 'periodo' : periodo.upper(), 'keycol': 'LOGIN', 'logins' : logins,
             'metricasconjuntas' : metricasconjuntas, 'kpis' : kpisvoz, 'comisionantes' : comisionantesvoz,
             'cuotas' : cuotasvoz, 'resultados' : resultadosvoz}

panelvoz = loader.prepareHistoricalFiles(paramsvoz)
panelvoz.to_csv(posixpath.join(testpath, month + '_panelvoz_gc.csv'),encoding='utf-8-sig')