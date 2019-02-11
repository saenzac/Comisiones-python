from Loader import fileloader as fl
from Loader import dfutils
import xlwings as xw
import pandas as pd

month = '201901'

inifile = fl.ReadIniFile()
defaultpath = inifile.getDefaultPath()
testpath = inifile.getTestPath()
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

#The final order the columns written in the excel file will have
columns_resultant = ['DNI','PADRON_FECHA_DE_INGRESO', 'FECHA_DE_INICIO_EN_EL_PUESTO','NOMBRE_DEL_PUESTO','FECHA_CESE']

# Generating padron empleados and ceses Dataframes
padron_df = loader.loadFile('Padron_Empleados')
padron_df_short = padron_df[ columns_resultant[0:4] ]

ceses_df = loader.loadFile('Ceses')
ceses_df_short = ceses_df[[ columns_resultant[0], columns_resultant[4] ]]

# Iteration over the comisiones Excel workbooks
inis = ['Comisionantes_Plataformas_All' , 'Comisionantes_CAL_All', 'Comisionantes_Fidelizacion_Telemarketing_All', 'Comisionantes_Soporte_Empresas_All',
     'Comisionantes_GrandesCuentas_All', 'Comisionantes_SolucionesNegocio_All','Comisionantes_Pymes_All']

for i in range(len(inis)):
  # Generating the dataframes.
  comisionantes_df = loader.loadFile(inis[i])
  col_index = dfutils.getExcelColIndexFromDF(comisionantes_df, 'DNI2')
  row_len = len(comisionantes_df.index)
  comisionantes_params = loader.getParameters()
  comis_skiprows = comisionantes_params['skiprows']
  file = loader.getFileList()[0]
  comisionantes_df_short = comisionantes_df[[columns_resultant[0]]].copy()

  # Merging the dataframes
  merged_1 = pd.merge(comisionantes_df_short, padron_df_short, how='left',left_on='DNI', right_on = 'DNI')
  merged_2 = pd.merge(merged_1, ceses_df_short, how='left',left_on='DNI', right_on = 'DNI')
  # Rename DNI -> DNI2 because the column name already exists in the sheet
  merged_2.rename(columns={'DNI':'DNI2'},inplace=True)

  # Opening the workbook and referencing to 'comisionantes' worksheet
  wb = xw.Book(file)
  comis_sheet = wb.sheets('Comisionantes')

  # Pasting the resultant dataframe of the merge operations  to the Excel sheet
  comis_sheet.range((comis_skiprows + 1,col_index),(row_len + comis_skiprows + 1,col_index)).number_format = '@'
  comis_sheet.range((comis_skiprows + 1,col_index)).options(pd.DataFrame, index=False).value = merged_2

  wb.save()
  wb.close()
  