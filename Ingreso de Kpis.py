from Loader import fileloader as fl
from Loader import datahandledatabase as dhdb
from Loader import datacompute as dc
from Loader import datapreparation as dp
import pandas as pd
import numpy as np
from datetime import datetime
import time
import posixpath

month = '201901'

inifile = fl.ReadIniFile()

defaultpath = inifile.getDefaultPath()
testpath = inifile.getTestPath()

# Configurando los archivos de configuración para importar archivos tipo : xlsx, txt, ini
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)


"""
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

"""


# Carga de Panel de voz
# *************************
# Agregar mes enero faltante en cuotas de GC
# Asegúrate en colocar 0 en la cabecera de los resultados y eliminar toda la data fuera del formato

# """
logins = loader.loadFile('Logins')  # ojo con tener logins duplicados. Sale errores raros
metricasconjuntas = loader.loadFile('Metricas_conjuntas')
kpisvoz = loader.loadFile('Kpis_voz')
comisionantesvoz = loader.loadFile('Comisionantes_voz')
cuotasvoz = loader.loadHistoricalFile('Cuotas')

# """

periodo = loader.getPeriodo()

# Preparar los resultados antes de enviar el cerrado
# GCE_Resultados debe tener dos o mas Hojas sino lanza error

#resultadosvoz = loader.loadHistoricalFile('Resultados')  # Por precaución eliminar tres columnas posteriores a dic-18.
resultadosvoz = pd.DataFrame() # Usar si no hay resultados

#churn = loader.loadHistoricalFile('Churn') #todabia no ponen las cuotas
#calidad = loader.loadHistoricalFile('Calidad') #todavia no envia nancy

# qnp = loader.loadHistoricalFile('QNP')
# tracking = loader.loadHistoricalFile('Tracking') # Asegurarse el nombre del kpi en resultados
# epa = loader.loadHistoricalFile('EPA') # Asegurarse el nombre del kpi en resultados

#resultadosvoz = resultadosvoz.append(churn, ignore_index=True)
#resultadosvoz = resultadosvoz.append(calidad, ignore_index=True)

# resultadosvoz = resultadosvoz.append(qnp, ignore_index = True)
# resultadosvoz = resultadosvoz.append(tracking, ignore_index = True)
# resultadosvoz = resultadosvoz.append(epa, ignore_index = True)

# """


paramsvoz = {'tipo' : 'voz', 'periodo' : periodo.upper(), 'keycol': 'LOGIN', 'logins' : logins,
             'metricasconjuntas' : metricasconjuntas, 'kpis' : kpisvoz, 'comisionantes' : comisionantesvoz,
             'cuotas' : cuotasvoz, 'resultados' : resultadosvoz}


panelvoz = loader.prepareHistoricalFiles(paramsvoz)
panelvoz.to_csv(posixpath.join(testpath, month + '_panelvoz.csv'),encoding='utf-8-sig')