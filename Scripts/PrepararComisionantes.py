from Loader import fileloader_proto as fl
import pandas as pd
import posixpath

month='201906'

inifile = fl.ReadIniFile(mercado="empresas")
section = fl.SectionObj(inifile,"Planillas",month)
empresas_mainpath = section.getParameter('mainpath_mercado') + '/Planillas'
loader = fl.LoadFileProcess(section)

planillas = loader.loadFile()

writer = pd.ExcelWriter(posixpath.join(empresas_mainpath,month + "_Consolidado_Planillas_201906_testborrar.xlsx"), engine='xlsxwriter')
planillas.to_excel(writer, sheet_name='Sheet1')
writer.save()