import logging

from Loader import fileloader_proto as fl
from Loader import dfutils
import xlwings as xw
import pandas as pd
import numpy as np
import posixpath

logger = logging.getLogger("")
logger.setLevel(logging.INFO)

"""
BONO TRIMESTRAL
Archivos de comisiones/secciones ini necesarios:
Comisionantes_GrandesCuentas_All
Comisionantes_Pymes_All

Puestos para los que aplica segun esquema actual:

Consultor Ventas Directas
Consultor Ventas Directas Senior
Consultores Captura Regiones
Ejecutivo Desarrollo de Negocio
Ejecutivo Desarrollo de Negocio Senior
Ejecutivo de Desarrollo Pyme Regiones
Ejecutivo de Regiones
Ejecutivo Remoto Regiones

Para bono trimestral importan las columnas de:
CAPTURA4.2 = resultado de VAS
CAPTURA4.3 = cumplimiento de VAS
GESTION1.3 = cumplimieento de CHURN
CAPTURA
"""

#Mes actual, a partir de esta cadena de texto se calculan las cadenas de los 2 meses anteriores
month = '201905'

inifile = fl.ReadIniFile(mercado="empresas")

cols = ["DNI","LOGIN","GERENCIA1","GERENCIA2","ZONA","DEPARTAMENTO","POSICIÓN","NOMBRES","PORCENTAJE_TOTAL_PONDERADO",
        "COMISIÓN","COMISION_ADICIONAL","PAGO","CAPTURA.1","CAPTURA 4.2","CAPTURA 4.3","GESTIÓN 1.3","Meses en el Puesto"]
filt1 = ['CONSULTOR VENTAS DIRECTAS','CONSULTOR SENIOR VENTAS DIRECTAS','CONSULTOR DE NEGOCIOS REGIONES',
         'EJECUTIVO DE DESARROLLO PYME','EJECUTIVO DE DESARROLLO PYME SENIOR','EJECUTIVO DE DESARROLLO PYME REGIONES',
         'EJECUTIVO DE REGIONES','EJECUTIVO DE REGIONES REMOTO']

def getMonthStringFromInt(mes_int):
  if mes_int < 10:
    mes_str = '0' + str(mes_int)
  else:
    mes_str = str(mes_int)
  return mes_str

#Calculamos las cadenas de texto que representan a los 2 meses anteriores
mes = int(month[-2:])
mes1 = mes - 1
mes2 = mes1 - 1
year = month[0:4]
mes1_str = getMonthStringFromInt(mes1)
date1_str = year + mes1_str
mes2_str = getMonthStringFromInt(mes2)
date2_str = year + mes2_str

### Mes Actual
# Cargamos los dataframes de Grandes Clientes y Pymes. Seleccionamos solo algunas columnas de interes (variable 'cols')
section2 = fl.SectionObj(inifile,"Comisionantes_GrandesCuentas_All",month)
section2.setParameter('cols',cols)
section2.setParameter('allcols',0)
section2.setParameter('presetsheet','Comisionantes')
loader2 = fl.LoadFileProcess(section2)
comisionantes_gc_df = loader2.loadFile()

section3 = fl.SectionObj(inifile,"Comisionantes_Pymes_All",month)
section3.setParameter('cols',cols)
section3.setParameter('allcols',0)
section3.setParameter('presetsheet','Comisionantes')
loader3 = fl.LoadFileProcess(section3)
comisionantes_pymes_df = loader3.loadFile()

# Concatenamos ambos dataframes y filtramos solo los puestos para los que aplica el bono trimestral.
#Además renombramos algunas columnas a nombres entendibles
frames = [comisionantes_gc_df, comisionantes_pymes_df]
result = pd.concat(frames)
stage = result["POSICIÓN"].isin(filt1)
result_filt = result[stage]
result_filt.rename(columns={'CAPTURA_4.2':'RESULTADO_VAS', 'CAPTURA_4.3':'CUMPL_VAS','GESTIÓN_1.3':'CUMPL_CHURN'}, inplace=True)

#Esto se utilizara para cruzar con los meses anteriores, para que asi se considere unicamente la planilla del mes actual.
key_column = result_filt["DNI"]

### Mes anterior
# Cargamos los dataframes de Grandes Clientes y Pymes. Seleccionamos solo algunas columnas de interes (variable 'cols')
section2 = fl.SectionObj(inifile,"Comisionantes_GrandesCuentas_All",date1_str)
section2.setParameter('cols',cols)
section2.setParameter('allcols',0)
section2.setParameter('presetsheet','Comisionantes')
loader2 = fl.LoadFileProcess(section2)
comisionantes_gc__df = loader2.loadFile()

section3 = fl.SectionObj(inifile,"Comisionantes_Pymes_All",date1_str)
section3.setParameter('cols',cols)
section3.setParameter('allcols',0)
section3.setParameter('presetsheet','Comisionantes')
loader3 = fl.LoadFileProcess(section3)
comisionantes_pymes_df = loader3.loadFile()

# Concatenamos ambos dataframes y filtramos solo los puestos para los que aplica el bono trimestral
frames = [comisionantes_gc__df, comisionantes_pymes_df]
result = pd.concat(frames)
result_filt_mes1 = pd.merge(key_column.to_frame(), result, on='DNI', how='left')
#Eliminar duplicados porsiacaso.
result_filt_mes1= result_filt_mes1.drop_duplicates('DNI')

#stage = result["POSICIÓN"].isin(filt1)
#result_filt_mes1 = result[stage]
result_filt_mes1.rename(columns={'CAPTURA_4.2':'RESULTADO_VAS', 'CAPTURA_4.3':'CUMPL_VAS','GESTIÓN_1.3':'CUMPL_CHURN'}, inplace=True)

# Abrimos archivo de Bono Trimestral y escribimos los resultados
section4 = fl.SectionObj(inifile,"Bono_Trimestral",month)
bono_trimestral_filepath = section4.getParameter('filelist')[0]
wb = xw.Book(bono_trimestral_filepath)
mes0_sheet = wb.sheets("mes_actual")
mes1_sheet = wb.sheets("mes_ant")
mes2_sheet = wb.sheets("mes_ant_ant")

mes0_sheet.range('A2').options(index=False,header=False).value = result_filt
mes1_sheet.range('A2').options(index=False,header=False).value = result_filt_mes1