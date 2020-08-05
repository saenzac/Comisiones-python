
import datetime
import logging
import ecomis
from icalendar import Calendar, Event
#from Loader import fileloader_proto as fl
import posixpath

#: The logger instance
logger = logging.getLogger("juplogger")
logger.setLevel(logging.INFO)

class GenCalendar:
  """
  Class which reads an excel file and generate calendars for the months which are specified as columns.
  And the days are rows of those columns. Which each event is specified at the first column.
  The output of the results is <mainpath>/Calendarios, where mainpath depends if "Mercado" is "Empresas" or "Personas".
  """
  def __init__(self, month, year):

    self.month = month
    self.year = year

    self.events = []
    ''' List of dictionaries, where each dictionary is an event.
    '''

    inifile = ecomis.ReadIniFile(mercado="empresas")

    #: Dataframe of the Excel File called "..._Bases.xlsx"
    section = ecomis.SectionObj(inifile,"Bases_Que_Nos_Envian")
    mainpath = section.getParameter('mainpath_mercado')
    #: Resulting calendar files path
    self.calendar_output_path = posixpath.join(mainpath, "Calendarios")
    loader = ecomis.LoadFileProcess(section)
    self.bases = loader.loadFile()
    #self.bases = loader.loadFile('Bases_Que_Nos_Envian') #metodo deprecated
    self.bases = self.bases[self.bases['Considerar'] == 'SI']
    #self.bases = self.bases[self.bases['Calendario Reducido'] == 'SI']
    self.bases = self.bases.fillna(0) #fill all na values with 0

    #: List of the complete columns names.
    self.list_of_cols = self.bases.columns.tolist()


  def work(self):

    #Iterate filling the list of events : self.events
    for index, row in self.bases.iterrows():
      if row.Nombres == 0:
        owner = ""
      else:
        firstname = row.Nombres.split(" ")
        firstletter = firstname[0][0]
        
        secondname = row.Apellidos.split(" ")[0]
        owner = " (" + firstletter + ". " + secondname + ")"

      dict_ = {"summary": row.BaseId + owner,
               "dtstart": datetime.date(int(self.year), int(self.month), int(row.Dia))
              }
      self.events.append(dict_)

    cal = Calendar()
    for row in self.events:
      event = Event()
      event.add('summary', row['summary'])
      event.add('dtstart', row['dtstart'])
      cal.add_component(event)

    filename = 'empresas_' + str(self.month) + "_" + str(self.year) + '.ics'
    filepath = posixpath.join(self.calendar_output_path, filename)
    f = open(filepath, 'wb')
    f.write(cal.to_ical())
    f.close()


a = GenCalendar("08","2020")
a.work()
