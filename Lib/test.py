# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import logging
from colorama import Fore
import ipywidgets as widgets
from IPython.display import clear_output
from IPython.display import display
import ecomis
import pandas as pd
import numpy as np
import posixpath
from datetime import datetime
import time
import xlwings as xw
import copy

logger = logging.getLogger("juplogger")
handler = ecomis.LogViewver()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

month = '202005'#para cargar unitarios, disminuir 1 mes
month_unitario = 201908#poner un mes mas que el de arriba.

inifile = ecomis.ReadIniFile(mercado="empresas")
#parser = inifile.getIniFileParser()

# Configurando los archivos de configuración para administrar la base de datos
dbparser = inifile.getDbIniFileParser()
dbmanager = ecomis.DbDataProcess(month)
dbmanager.setParser(dbparser)
handler.show_logs()

# %% [markdown]
# *** Carga de Archivos ***
# 
# Asegurate que el directorio donde está el archivo este correcto
# 
# Asegurate que no haya pestañas ocultas en Bases GCP ni Plataformas. 

# %%
list_ops = ['Ceses', 'Jerarquia','Inar', 'Deacs', 'Ventas_SSAA_new', 'Paquetes_new','Unitarios','Bolsas','Bases_GCP','Garantias']

# Carga de Ceses
def load_ceses():
    section_ceses = ecomis.SectionObj(inifile,'Ceses',month)
    loader_ceses = ecomis.LoadFileProcess(section_ceses)
    dataf = loader_ceses.loadFile()
    return dataf

def load_targets():
    section_targets = ecomis.SectionObj(inifile,'Targets',month)
    loader_targets = ecomis.LoadFileProcess(section_targets)
    dataf = loader_targets.loadFile()
    return dataf

# Carga de Jerarquia
# Advertencia : Verificar que no se duplique
def load_jerarquia():
    section_jerarquia = ecomis.SectionObj(inifile,'Jerarquia',month)
    loader_jerarquia = ecomis.LoadFileProcess(section_jerarquia)
    dataf = loader_jerarquia.loadFile()
    return dataf


# Carga de Inar
# Preparación : Agregar la columna de Seguro si Gestión de Información no lo proporciona
def load_inar():
    section_inar = ecomis.SectionObj(inifile,'Inar',month)
    loader_inar = ecomis.LoadFileProcess(section_inar)
    dataf = loader_inar.loadFile()
    return dataf

# Carga Deacs
# Preparación :Completar dos columnas adicionales ver archivo anterior. Mensual y Quincenal
def load_deacs():
    section_deacs = ecomis.SectionObj(inifile,'Deacs',month)
    loader_deacs = ecomis.LoadFileProcess(section_deacs)
    dataf = loader_deacs.loadFile()
    return dataf

# Carga VAS
# Separar el archivo de RAS en 2: 
# 1 archivo con activaciones y otro con desactivaciones.
# Aca solo cargamos el archivo de desactivaciones
# Estado: OK
def load_vas():
    section_ventas_ssaa = ecomis.SectionObj(inifile,'Ventas_SSAA_new',month)
    loader_ventas_ssaa = ecomis.LoadFileProcess(section_ventas_ssaa)
    dataf = loader_ventas_ssaa.loadFile()

    dataf.replace(np.nan, '', regex=True, inplace=True) 
    #Eliminamos los contratos con algun valor en la columna NETO
    #dataf = dataf[ dataf['NETO'] == "" ]
    #Considerar únicamente los registros que sean “Si” en la columna “Considerar”, los demás se eliminan/ no se consideran.
    #dataf = dataf[ dataf['NO_CONSIDERAR'] == "" ]
    #Quitamos PAQUETE_DATOS
    #dataf = dataf[~(dataf['FAMILIA'] == "PAQUETE_DATOS") ]
    dataf.reset_index(drop = True, inplace = True)
    return dataf

# Carga Paquetes
# Verificar Fechas
def load_paquetes():
    section_paquetes = ecomis.SectionObj(inifile,'Paquetes_new',month)
    loader_paquetes = ecomis.LoadFileProcess(section_paquetes)
    paquetes = loader_paquetes.loadFile()
    return paquetes

# Carga de Comision Unitaria
# Advertencia : la lectura del unitario es en el tiempo Mes de Comisiones -1. 
# File :  _Comisiones Pymes
#def load_unitarios():
#    section_unitarios = ecomis.SectionObj(inifile,"Unitarios",month)
#    loader_unitarios = ecomis.LoadFileProcess(section_unitarios)
#    unitarios = loader_unitarios.loadFile()

#    unitarios = unitarios[unitarios['COMISION_UNITARIA']>0]
#    unitarios.reset_index(drop = True, inplace = True)
#    return unitarios

# Carga de Bolsas
# Preparación : Completar columnas numericas Cero
def load_bolsas():
    section_bolsas = ecomis.SectionObj(inifile,"Bolsas",month)
    loader_bolsas = ecomis.LoadFileProcess(section_bolsas)
    bolsas = loader_bolsas.loadFile()
    return bolsas

# Carga de GCP
# Preparación : Update despues de Ingresar el INAR mensual. Llenar carterización y dealer regiones
# File _ : _GCP Base Comisiones
def load_BasesGCP():
    section_base_pyme = ecomis.SectionObj(inifile,"Bases_GCP",month)
    loader_base_pyme = ecomis.LoadFileProcess(section_base_pyme)
    basesgcp = loader_base_pyme.loadFile()
    return basesgcp

# Carga de Garantias
# Preparación : Update despues de Ingresar el INAR mensual. Llenar carterización y dealer regiones
# File _ : _GCP Base Comisiones
def load_garantias():
    section_garantias = ecomis.SectionObj(inifile,"Garantias",month)
    loader_base_garantias = ecomis.LoadFileProcess(section_garantias)
    garantias = loader_base_garantias.loadFile()
    garantias['CODIGO_INAR'] = garantias['CODIGO_INAR'].str.strip()
    return garantias

# Carga de Subsidios
def load_subsidios():
    section_subsidios = ecomis.SectionObj(inifile,"Subsidios",month)
    loader_subsidios = ecomis.LoadFileProcess(section_subsidios)
    subsidios = loader_subsidios.loadFile()
    return subsidios
#soluciones de negocio en COMISION_UNITARIA, VENDEDOR, FECHA_PROCESO

# Preparación : Ninguna
# File : _Riesgos Actividad Lima, _Riesgos Actividad Regiones
#actividad = loader.loadFile('Actividad') #en deshuso


# Preparación : Completar dos columnas.Verificar Fechas. Periodo Mensual y Quincenal.
# File : _GCE Deacs SSAA
#section_deacs_ssaa = fl.SectionObj(inifile,"Deacs_SSAA_new",month)
#loader_deacs_ssaa = fl.LoadFileProcess(section_deacs_ssaa)
#deacs_ssaa = loader_deacs_ssaa.loadFile()

# Advertencia : Separar en dos Periodos debido a Equipos de Captura Quincenal, tomar sólo el quincenal del periodo anterior
# File : _GCP Base Quincenal Comisiones
#basesgcpquincenal = loader.loadFile('Bases_GCP_Quincenal') #no se esta usando

# Preparación : Ver Errores. Cargar a conveniencia
# Advertencia : Importar en caso no lo incluya Gestión de Información
# File : _GCE Base Comisiones
#basesgce = loader.loadFile('Bases_GCE') #no se esta usando

# %% [markdown]
# ** Ceses **
# 
# Usar el archivo de ceses que contenga los ceses hasta fin del mes de producción.
# 
# 

# %%
dataf = []
def show_details_callback(event):
  handler.clear_logs()
  section = ecomis.SectionObj(inifile,dd_gui.value,month)
  t = []
  t.append("Cols. a ser leidas: [" + ', '.join(section.getParameter("cols")) + "]")
  t.append("Cols. Date Type: [" + ', '.join(section.getParameter("colsdatetype")) + "]")
  t.append("Keyfile: [" + ', '.join(section.getParameter("keyfile")) + "]")
  t.append("Presetsheet: [" + ', '.join(section.getParameter("presetsheet")) + "]")
  t.append("Cols Converted: [" + ', '.join(section.getParameter("colsconverted")) + "]")
  t.append("Cols To Change: [" + ', '.join(section.getParameter("colstochange")) + "]")
  for i in t:
    logger.info(i)

list_ops = ['Ceses', 'Targets', 'Jerarquia','Inar', 'Deacs', 'Ventas_SSAA_new', 'Paquetes_new','Unitarios','Bolsas','Bases_GCP','Garantias','Subsidios']
def load_btn_callback(event):
    global dataf
    del dataf
    handler.clear_logs()
    if dd_gui.value == "Ceses":
        dataf = load_ceses()
    if dd_gui.value == "Targets":
        dataf = load_targets()
    elif dd_gui.value == "Jerarquia":
        dataf = load_jerarquia()
    elif dd_gui.value == "Inar":
        dataf = load_inar()
    elif dd_gui.value == "Deacs":
        dataf = load_deacs()
    elif dd_gui.value == "Ventas_SSAA_new":
        dataf = load_vas()
    elif dd_gui.value == "Paquetes_new":
        dataf = load_paquetes()
    #elif dd_gui.value == "Unitarios":
    #    dataf = load_unitarios()
    elif dd_gui.value == "Bases_GCP":
        dataf = load_BasesGCP()
    elif dd_gui.value == "Garantias":    
        dataf = load_garantias()
    elif dd_gui.value == "Subsidios":    
        dataf = load_subsidios()

def clear_callback(event):
    handler.clear_logs()

btn_ceses_ejecutar_gui = widgets.Button(
    description='Ejecutar Carga', disabled=False, button_style='info', tooltip='Ejecutar Carga Ceses', icon='' )
btn_ceses_detalles_gui = widgets.Button(
    description='Ver detalles de carga', disabled=False, button_style='info', tooltip='Ejecutar Carga Ceses', icon='' )
btn_clear_gui = widgets.Button(
    description='Clear', disabled=False, button_style='info', tooltip='Clear', icon='' )
dd_gui = widgets.Dropdown(
    options=list_ops,
    value='Ceses',
    description='Number:',
    disabled=False)

btn_ceses_ejecutar_gui.on_click(load_btn_callback)
btn_ceses_detalles_gui.on_click(show_details_callback)
btn_clear_gui.on_click(clear_callback)
display(dd_gui)
display(btn_ceses_ejecutar_gui)
display(btn_ceses_detalles_gui)
display(btn_clear_gui)
handler.show_logs()


# %%
dataf

# %% [markdown]
# ** Inar **
# 
# Preparación : Agregar la columna de Seguro si Gestión de Información no lo proporciona
# File :  _Planeamiento Inar Empresas

# %%
# Testing Dataframes

#jerarquia.dtypes

#inar.dtypes
#deacs.dtypes 

#ventas_ssaa.dtypes
#deacs_ssaa.dtypes
#dataf.dtypes
#bolsas.dtypes

#basesgcp.dtypes
#basesgcp.describe()
#basesgcp.describe(include = ['O'])
#basesgce.dtypes
#actividad.dtypes
#basesgcpquincenal.dtypes
#jerarquia.dtypes

#unitarios.dtypes

# Looking the df
#unitarios
#unitarios[unitarios['PERIODO_ACTIVACION']==201901]
#basesgcp[basesgcp['SEGMENTO_ACCESS'].notnull()].head()
#paquetes.head()
#ceses.head()
#basesgcp
#ventas_ssaa


# %%
#1. Carga de Data en Base de Datos

#dbmanager.dbOperation('insert','tblJerarquia',dataf) #ok
#dbmanager.dbOperation('insert','tblVentas',dataf)#ok
#dbmanager.dbOperation('insert','tblDeacs',dataf)# ok
#dbmanager.dbOperation('insert','tblGarantias',dataf)# ok

#dbmanager.dbOperation('insert','tblVentaSSAANew',dataf) # ok
#dbmanager.dbOperation('insert','tblDeacSSAA',deacs_ssaa) FALTA, investigar
#dbmanager.dbOperation('insert','tblPaquetes',dataf) # no se hizo, parece en deshuso

#********************************************
#2.  Update de tablas de Ventas y Deacs (deben estar en memoria)

#dbmanager.dbOperation('update','Ceses', dataf) #ok
#dbmanager.dbOperation('update','UpdateTargetEmpleado', dataf) #ok
#dbmanager.dbOperation('update','UpdateSubsidiosEmpleados', dataf) #ok

#dbmanager.dbOperation('update','Bolsas',bolsas) #no hubo
#dbmanager.dbOperation('update','SumVentaSSAA',dataf) #
#dbmanager.dbOperation('update','Paquetes') # no se hizo, parece en deshuso
#dbmanager.dbOperation('update','Bases_GCP', dataf) # 

#3. Calculando el Gross Comisión y las Reversiones (Obligatorio al final de la carga)
#*************************************************
#start_time = time.time()

#dbmanager.dbOperation('update','Gross_Comision') #no hecho, no se usa al parecer
dbmanager.dbOperation('update','Reversiones') # ok

#time_consuming = (time.time() - start_time) # In minutes
#print("--- %s seconds ---" % time_consuming)

#handler.show_logs()

# DEPRECATED:
#dbmanager.dbOperation('update','Actividad', actividad) #en deshuso
#dbmanager.dbOperation('update','Bases_GCP_Quincenal',basesgcpquincenal) # actualizar con data quincenal,
#dbmanager.dbOperation('update','Bases_GCE',basesgce) # Usar a conveniencia,

