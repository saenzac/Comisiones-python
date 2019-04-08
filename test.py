import datetime
from icalendar import Calendar, Event

import logging

from Loader import fileloader as fl
from Loader import dfutils
import xlwings as xw
import pandas as pd
import numpy as np

logger = logging.getLogger("")
logger.setLevel(logging.INFO)


month = '201903'

inifile = fl.ReadIniFile(mercado="personas")
defaultpath = inifile.getDataPath()
testpath = inifile.getTestPath()
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

# Dataframe de archivo comisiones pestaña "Leyenda" de la ruta de Reporte de Productividad
bases = loader.loadFile('Bases_Que_Nos_Envian')
bases = bases[bases['CALENDARIO'] == 'SI']
bases = bases[['BASE', 'DNI', 'NOMBRES', 'APELLIDOS', 'DIA', 'MES', 'CALENDARIO']]
bases = bases.fillna(0)
#data is a array of dictionaries

data = []

for index, row in bases.iterrows():
  if row.NOMBRES == 0:
    owner = ""
  else:
    firstname = row.NOMBRES.split(" ")
    if len(firstname) > 1:
      firstletter = firstname[0][0]
    else:
      firstletter = firstname[0][0]
    secondname = row.APELLIDOS.split(" ")[0]
    owner = " (" + firstletter + ". " + secondname + ")"

  dict_ = {"summary": row.BASE + owner,
           "dtstart": datetime.date(2019, int(row.MES), int(row.DIA))
          }
  data.append(dict_)

cal = Calendar()

for row in data:
  event = Event()
  event.add('summary', row['summary'])
  event.add('dtstart', row['dtstart'])
  cal.add_component(event)

f=open('course_schedule.ics', 'wb')
f.write(cal.to_ical())
f.close()
