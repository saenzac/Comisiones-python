import numpy as np
import pandas as pd
from pandas import Series, DataFrame
from win32com.client import dynamic

def getExcelColIndexFromDF(df, colname):
  return df.columns.get_loc(colname) + 1

def getOpenedWorkbooksNames():
  com_app = dynamic.Dispatch('Excel.Application')
  com_wbs = com_app.Workbooks
  wb_names = [wb.Name for wb in com_wbs]
  return wb_names