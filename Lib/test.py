# -*- coding: utf-8 -*-
import ecomis
import pandas as pd
import numpy as np
from datetime import datetime
import time
import posixpath
import logging

logger = logging.getLogger("juplogger")
handler = ecomis.LogViewver()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

month = '202011'

inifile = ecomis.ReadIniFile(mercado="empresas")
defaultpath = inifile.getDataPath()
testpath = inifile.getTestPath()

handler.show_logs()

# Configurando los archivos de configuración para administrar la base de datos
dbparser = inifile.getDbIniFileParser()
dbmanager = ecomis.DbDataProcess(month)
dbmanager.setParser(dbparser)

# Carga de Panel de Plataformas Comerciales: 
# Preparación de Data : Quitar formulas y quitar pestañas ocultas. Si hay intercambio de supervisores (o nuevo supervisor) 
# actualizar logins equivalentes y metricas conjuntas
#NOTA: Los nombres de las personas en el archivo de comisionantes de estar en mayusuclas.

#Carga DF de archivo ...\ecomis\Data\logins equivalentes.xlsx
section_logins = ecomis.SectionObj(inifile,'Logins',month)
logins = ecomis.LoadFileProcess(section_logins).loadFile()

#Carga DF de archivo ...\ecomis\Data\metricas conjuntas.xlsx
section_metricasconjuntas = ecomis.SectionObj(inifile,'Metricas_conjuntas',month) #debe estar en mayus
metricasconjuntas = ecomis.LoadFileProcess(section_metricasconjuntas).loadFile()

#Carga pestaña Leyenda de archivo de comisiones
section_kpispltfrs = ecomis.SectionObj(inifile,'Kpis_plataformas',month) #debe estar en mayus
kpispltfrs = ecomis.LoadFileProcess(section_kpispltfrs).loadFile()
#Convertimos todo a mayuscula
kpispltfrs = kpispltfrs.applymap(lambda s:s.upper() if type(s) == str else s)

#Carga pestaña Comisionantes de archivo de comisiones
section_comisionantespltfrs = ecomis.SectionObj(inifile,'Comisionantes_plataformas',month)
comisionantespltfrs = ecomis.LoadFileProcess(section_comisionantespltfrs).loadFile()

#Carga panel de plataformas comerciales
section_panelpltfrs0 = ecomis.SectionObj(inifile,'Panel_plataformas',month) #debe estar en mayus
panelpltfrs0 = ecomis.LoadFileProcess(section_panelpltfrs0).loadFile()
#Convertimos datos de panel a matuscula
panelpltfrs0["DATOS"] = panelpltfrs0["DATOS"].str.upper()

periodo = section_logins.getParameter('periodo')

paramspltfrs= {'tipo' : 'plataformas',
               'periodo' : periodo.upper(),
               'keycol': 'DATOS',
               'logins' : logins, 
               'metricasconjuntas' : metricasconjuntas,
               'kpis' : kpispltfrs,
               'comisionantes' : comisionantespltfrs, 
               'cuotas' : panelpltfrs0,
               'resultados' : None}

panelpltfrs = (ecomis.LoadFileProcess(section_panelpltfrs0)).prepareHistoricalFiles(paramspltfrs)
panelpltfrs.to_csv(posixpath.join(testpath, month + '_panelpltfrs.csv'),encoding='utf-8-sig')

handler.show_logs()