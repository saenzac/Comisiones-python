import numpy as np
import pandas as pd
from pandas import Series, DataFrame


def getExcelColIndexFromDF(df, colname):
  return df.columns.get_loc(colname) + 1
