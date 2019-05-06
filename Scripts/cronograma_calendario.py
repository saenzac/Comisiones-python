import datetime
from icalendar import Calendar, Event
import logging
from Loader import fileloader as fl
import posixpath

#: The logger instance
logger = logging.getLogger("")
logger.setLevel(logging.INFO)

class GenCalendar:
  """
  Class which reads an excel file and generate calendars for the months which are specified as columns.
  And the days are rows of those columns. Which each event is specified at the first column.
  The output of the results is <mainpath>/Calendarios, where mainpath depends if "Mercado" is "Empresas" or "Personas".
  """
  def __init__(self):

    #: Index number where the columns become dates
    self.startdateindex = 10

    self.alldicts = {}
    ''' Dictionary of dictionaries. Each element is a dictionary whose value is a python list and the key is the string
        of the form <month>-<year>, e.g. "5-2019".  And each element of the list is a dictionary. It has the following form:
        alldicts = { list1 = [ dict1={} ] }
    '''

    #: List of calendars
    self.calendars = []

    #: Prefix of the loaded  excel file. It is mandatory for now. TODO: Make it optional
    self.month = '201904'

    inifile = fl.ReadIniFile(mercado="empresas")
    defaultpath = inifile.getDataPath()
    parser = inifile.getIniFileParser()

    loader = fl.LoadFileProcess(self.month)
    loader.setParser(parser)
    loader.setDefaultPath(defaultpath)

    mainpath = parser['DEFAULT']['mainpath_esp']
    #: Resulting calendar files path
    self.calendar_output_path = posixpath.join(mainpath, "Calendarios")

    #: Dataframe of the Excel File called "..._Bases.xlsx"
    self.bases = loader.loadFile('Bases_Que_Nos_Envian2')
    self.bases = self.bases[self.bases['Calendario Reducido'] == 'SI']
    self.bases = self.bases.fillna(0) #fill all na values with 0

    #: List of the complete columns names.
    self.list_of_cols = self.bases.columns.tolist()

    #: List of  columns names of the dates only.
    self.datecols = self.list_of_cols[self.startdateindex:]

  def work(self):
    for i in self.datecols:
      #Only for convenience we remove all the columns dates and put only the one referenced by the "i" iterator.
      cols = self.list_of_cols[0:self.startdateindex] + [i]
      bases1 = self.bases[cols]

      #define the dictionary for the "i" month / period.
      self.alldicts[str(i.month) + "-" + str(i.year)] = []

      #Iterate filling the dictionary
      for index, row in self.bases.iterrows():
        if row.Nombres == 0:
          owner = ""
        else:
          firstname = row.Nombres.split(" ")
          if len(firstname) > 1:
            firstletter = firstname[0][0]
          else:
            firstletter = firstname[0][0]
          secondname = row.Apellidos.split(" ")[0]
          owner = " (" + firstletter + ". " + secondname + ")"

        dict_ = {"summary": row.Base + owner,
                 "dtstart": datetime.date(2019, int(i.month), int(row[i]))
                }
        self.alldicts[str(i.month) + "-" + str(i.year)].append(dict_)

      cal = Calendar()
      for row in self.alldicts[str(i.month) + "-" + str(i.year)]:
        event = Event()
        event.add('summary', row['summary'])
        event.add('dtstart', row['dtstart'])
        cal.add_component(event)

      self.calendars.append(cal)

      filename = 'empresas_' + str(i.month) + "_" + str(i.year) + '.ics'
      filepath = posixpath.join(self.calendar_output_path, filename)
      f = open(filepath, 'wb')
      f.write(cal.to_ical())
      f.close()


a = GenCalendar()
a.work()