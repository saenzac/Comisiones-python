import datetime
from icalendar import Calendar, Event

import logging

from Loader import fileloader as fl
from Loader import dfutils
import xlwings as xw
import pandas as pd
import re

#Initializing the logging instance
logger = logging.getLogger("")
logger.setLevel(logging.INFO)

month = '201903'
months = []

inifile = fl.ReadIniFile(mercado="empresas")
defaultpath = inifile.getDataPath()
testpath = inifile.getTestPath()
parser = inifile.getIniFileParser()

loader = fl.LoadFileProcess(month)
loader.setParser(parser)
loader.setDefaultPath(defaultpath)

# Load dataframe of "bases" file
bases = loader.loadFile('Bases_Que_Nos_Envian')
bases = bases[bases['CALENDARIO'] == 'SI']

# Generate a list of the headers which contains dates:
list_of_cols = bases.columns.tolist()
r = re.compile("^[a-zA-Z]{3}-\d{2}$")
df_dates_cols = list(filter(r.match, list_of_cols))

bases = bases[['BASE', 'DNI', 'NOMBRES', 'APELLIDOS', 'DIA', 'MES', 'CALENDARIO']]
bases = bases.fillna(0)
#data is a array of dictionaries



alldicts = {}
alldicts["abr-19"] = []

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
  alldicts["abr-19"].append(dict_)

cal = Calendar()

for row in data:
  event = Event()
  event.add('summary', row['summary'])
  event.add('dtstart', row['dtstart'])
  cal.add_component(event)

f=open('course_schedule.ics', 'wb')
f.write(cal.to_ical())
f.close()
