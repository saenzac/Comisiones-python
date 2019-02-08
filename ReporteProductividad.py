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

month = '201707'

inifile = fl.ReadIniFile()
defaultpath = inifile.getDefaultPath()
testpath = inifile.getTestPath()
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

pesospltfrs = loader.loadFile('Pesos_plataformas')
comisionantespltfrs = loader.loadFile('Comisionantes_plataformas_rproductividad')

#Abriendo archivo y haciendo hoja 'comisionantes' activa
file = loader.getFileList()[0]
wb = xw.Book(file)
comis_sheet = wb.sheets('Comisionantes')
leyenda_sheet = wb.sheets('Leyenda')


### Inmovilizando valores de porcentajes ponderados
#Obteniendo indice de la columna de porcentaje ponderado
ponderado_cindex = comisionantespltfrs.columns.get_loc("PORCENTAJE_TOTAL_PONDERADO") + 1
# Inmovilizando columna ponderados
lastrow = comis_sheet.api.Cells(65536,ponderado_cindex).End(xw.constants.Direction.xlUp).Row
comis_sheet.range((1,ponderado_cindex)).options(transpose=True).value = comis_sheet.range((1,ponderado_cindex),(lastrow,ponderado_cindex)).value



### Actualizando Pesos
pesos_df = pesospltfrs
pesos_df = pesos_df[pesos_df['ITEM'] != 'item']
pesos_df = pesos_df[~pesos_df['ITEM'].isnull()]
pesos_df = pesos_df[pesos_df['ITEM'] < 3000]
pesos_df = pesos_df[pesos_df['ITEM'] > 2000]


captura_df=pesos_df.filter(regex=("VENTA.*"))
headers = captura_df.columns.values
captura_df=captura_df.fillna(0) #fill empty spaces read as nan to zeros
captura_mat=captura_df.as_matrix() #mxn
sum_captura_mat_1 = np.sum(captura_mat,axis=1) #1xm, sum all the rows
sum_captura_mat_2 = 1/ sum_captura_mat_1
sum_captura_mat = np.nan_to_num(sum_captura_mat_2,copy=True)
diag_captura_mat = np.diag(sum_captura_mat)#mxm
t_captura_mat = np.dot(diag_captura_mat,captura_mat) #mxm x mxn = mxn
captura_df_conv =  pd.DataFrame(t_captura_mat,columns=headers)

gestion_df=pesos_df.filter(regex=("GESTIÓN.*"))
headers = gestion_df.columns.values
gestion_df=gestion_df.fillna(0) #fill empty spaces read as nan to zeros
gestion_mat=gestion_df.as_matrix() #mxn
sum_gestion_mat_1 = np.sum(gestion_mat,axis=1) #1xm, sum all the rows
sum_gestion_mat_2 = 1/ sum_gestion_mat_1
sum_gestion_mat = np.nan_to_num(sum_gestion_mat_2,copy=True)
diag_gestion_mat = np.diag(sum_gestion_mat)#mxm
t_gestion_mat = np.dot(diag_gestion_mat,gestion_mat) #mxm x mxn = mxn
gestion_df_conv =  pd.DataFrame(t_gestion_mat,columns=headers)

desarrollo_df=pesos_df.filter(regex=("DESARROLLO.*"))
headers = desarrollo_df.columns.values
desarrollo_df=desarrollo_df.fillna(0) #fill empty spaces read as nan to zeros
desarrollo_mat=desarrollo_df.as_matrix() #mxn
sum_desarrollo_mat_1 = np.sum(desarrollo_mat,axis=1) #1xm, sum all the rows
sum_desarrollo_mat_2 = 1/ sum_desarrollo_mat_1
sum_desarrollo_mat = np.nan_to_num(sum_desarrollo_mat_2,copy=True)
diag_desarrollo_mat = np.diag(sum_desarrollo_mat)#mxm
t_desarrollo_mat = np.dot(diag_desarrollo_mat,desarrollo_mat) #mxm x mxn = mxn
desarrollo_df_conv =  pd.DataFrame(t_desarrollo_mat,columns=headers)

df_conv = pd.concat([captura_df_conv,gestion_df_conv,desarrollo_df_conv],axis=1)


#Buscar columna CAPTURA 1
col_pesos = pesospltfrs.columns.get_loc("VENTA_1") + 1
#Buscar fila primera ocurrencia de ITEM
row_pesos = min(pesospltfrs.index[pesospltfrs['ITEM'] == 'item'].tolist()) + 4

wb.sheets.add('backup_pesos')
backup_pesos_sheet = wb.sheets('backup_pesos')
backup_pesos_sheet.range('A1').value = pesos_df

leyenda_sheet.range(row_pesos,col_pesos).options(pd.DataFrame, index=False).value = df_conv

a=0