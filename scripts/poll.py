#!/usr/bin/python

# This script is meant to pull information from the Google Maps Engine
# used internally by the Track a Train Webapp
# Ideally, pulling directly from the source to generate the map will be 
# possible.

# Used to delay updates
from time import sleep
from time import time
from datetime import datetime, date, time as daytime, timedelta

# Used to poll the data files
from urllib2 import urlopen

# Used to open numbered file descriptors to write to
# for use with ArcLaunch
from os import fdopen, environ
from sys import argv

# Used to store information long term
from MySQLdb import connect

# Needs to close gracefully with SIGINT
import signal

from json import load as jsonload, loads as jsonloads, dumps as jsonsaves


class MySQLObject(object):
  TableName = ""
  Fields = []
  FieldDefinition = []
  FieldDefault = {}
  WriteFields = None
  ReadFields = None
  Identity = None
  Keys = None
  Mapping = None
  data = {}
  def __init__(self, info = None):
    self.data = dict()
    if self.Mapping and info:
      for key, value in self.Mapping.iteritems():
        self.data[key] = info.get(value)
    if not self.WriteFields:
      self.WriteFields = self.Fields
    if not self.ReadFields:
      self.ReadFields = self.Fields
    if not self.Identity:
      self.Identity = self.Fields
    if not self.Keys:
      self.Keys = self.Fields
  
  def __eq__(self, other):
    # Should efficiently cover most cases
    if hash(self) != hash(other):
      return False
    # Checks exceptional cases
    for key in self.Identity:
      if self.data.get(key) != other.data.get(key):
        return False
    return True
        
  def __hash__(self):
    ax = hash(self.TableName)
    for key in self.Identity:
      value = self.data.get(key)
      ax += hash(value) if value else 0
    return ax
  
  def subList(self, num):
    return ", ".join(["%s"] * num)
  
  def getDataList(self, names):
    ret = []
    for name in names:
      ret.append(self.data.get(name))
    return ret
  
  def initialize(self, db, existing):
    if self.TableName in existing:
      return
    cur = db.cursor()
    rep = [self.TableName]
    # Note: FieldDefinition is not escaped
    # so don't allow the field definitions to be publicly modified
    cur.execute( \
      "CREATE TABLE " + self.TableName + \
      " (" + ", ".join(self.FieldDefinition) + ");")
    cur.close()
  
  def destroy(self, db, existing):
    if self.TableName not in existing:
      return
    cur = db.cursor()
    cur.execute("DROP TABLE " + self.TableName)
    cur.close()
    db.commit()
  
  def write(self, db):
    cur = db.cursor()
    slots = self.subList(len(self.WriteFields))
    rep = [self.TableName]
    rep.extend(self.WriteFields)
    rep.extend(self.getDataList(self.WriteFields))
    cur.execute( \
      "INSERT INTO %s (" + slots + ") VALUE " + \
      "(" + slots + ")",
      rep
      )
    cur.close()
    db.commit()
  
  def copy(self):
    # Note: copy just creates a MySQLObject
    # if the object needs to be used as more than just a MySQLObject
    # the copy method needs to be overriden
    dup = MySQLObject()
    dup.TableName = self.TableName
    dup.Fields = self.Fields
    dup.FieldDefinition = self.FieldDefinition
    dup.FieldDefault = self.FieldDefault
    dup.WriteFields = self.WriteFields
    dup.ReadFields = self.ReadFields
    dup.Keys = self.Keys
    dup.Identity = self.Identity
    dup.Mapping = self.Mapping
    dup.data = self.data.copy()
    return dup
  def writeStream(self, f):
    # Writes the data to a file-like object
    # as a JSON dict
    return jsonsaves(self.data)
  @staticmethod
  def getExistingTables(db):
    cur = db.cursor()
    cur.execute("SHOW TABLES;");
    tables = []
    for result in cur:
      tables.append(result[0])
    cur.close()
    return tables

# Order should not matter
# Of greater importance than order is to check membership
class MySQLObjectGroup(set):
  base = None
  def __init__(self, b, s = set()):
    self.base = b
    super(MySQLObjectGroup, self).__init__(s)

  def union(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).union(other))

  def __or__(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).__or__(other))
  
  def intersection(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).intersection(other))
  
  def __and__(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).__and__(other))

  def difference(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).difference(other))

  def __sub__(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).__sub__(other))

  def symmetric_difference(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).symmetric_difference(other))

  def __xor__(self, other):
    return MySQLObjectGroup(self.base, 
      super(MySQLObjectGroup, self).__xor__(other))

  def write(self, db):
    slots = self.base.subList(len(self.base.WriteFields))
    rep = []
    for entry in self:
      ordered = []
      for key in self.base.WriteFields:
        ordered.append(entry.data[key])
      rep.extend(ordered)
    if len(self):
      cur = db.cursor()
      cur.execute( \
        "INSERT INTO " + self.base.TableName + " (" + \
        ", ".join(self.base.WriteFields) + ") VALUES " + \
        ", ".join(["(" + slots + ")"] * len(self)) + \
        ";", rep);
      cur.close()
      db.commit()
  def read(self, db):
    # Appends the results to the end of the existing list
    cur = db.cursor()
    cur.execute( \
      "SELECT " + ", ".join(self.base.ReadFields) + \
      " FROM " + self.base.TableName + ";")
    for fetch in cur:
      entry = self.base.copy()
      for idx, field in enumerate(self.base.ReadFields):
        entry.data[field] = fetch[idx]
      self.add(entry)
    cur.close()
  def writeStream(self, f):
    # Writes the group of objects as JSON objects
    justdata = []
    for entry in self:
      justdata.append(entry.data)
    return jsonsaves(justdata)

class Train(MySQLObject):
  TableName = "trains"
  Fields = [ \
    "TrainNum", \
    "RouteName", \
    "OrigTime", \
    "OrigStation", \
    "DestStation" \
    ]
  FieldDefinition = [ \
    "TrainNum INT", \
    "RouteName VARCHAR(255)", \
    "OrigTime DATETIME", \
    "OrigStation CHAR(3)", \
    "DestStation CHAR(3)", \
    "CONSTRAINT NumOrig PRIMARY KEY (TrainNum, OrigTime)"
    ]
  Identity = [ \
    "TrainNum", \
    "OrigTime" \
    ]
  Mapping = { \
    "TrainNum": "TrainNum", \
    "RouteName": "RouteName", \
    "OrigTime": "OrigTime", \
    "OrigStation": "OrigCode", \
    "DestStation": "DestCode" \
    }

class TrainReading(MySQLObject):
  TableName = "readings"
  Fields = [ \
    "TrainNum", \
    "OrigTime", \
    "Latitude", \
    "Longitude", \
    "Time", \
    "Speed", \
    "Heading", \
    "State"]
  FieldDefinition = [
    "TrainNum INT", \
    "OrigTime DATETIME", \
    "Latitude DOUBLE", \
    "Longitude DOUBLE", \
    "Time DATETIME", \
    "Speed DOUBLE", \
    "Heading ENUM('', 'N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW')", \
    "State ENUM('', 'Predeparture', 'Active', 'Completed')", \
    "CONSTRAINT NumOrigTime PRIMARY KEY (TrainNum, OrigTime, Time)" \
    ]
  Identity = [ \
    "TrainNum", \
    "OrigTime", \
    "Time" \
    ]
  Mapping = { \
    "TrainNum": "TrainNum", \
    "OrigTime": "OrigTime", \
    "Latitude": "Latitude", \
    "Longitude": "Longitude", \
    "Time": "RecordTime", \
    "Speed": "Velocity", \
    "Heading": "Heading", \
    "State": "TrainState" \
    }

class TrainStop(MySQLObject):
  TableName = "stops"
  Fields = [ \
    "StationCode", \
    "TrainNum", \
    "OrigTime", \
    "ScheduledArrival", \
    "ScheduledDeparture", \
    "ActualArrival", \
    "ActualDeparture" \
    ]
  FieldDefinition = [ \
    "StationCode CHAR(3)", \
    "TrainNum INT", \
    "OrigTime DATETIME", \
    "ScheduledArrival DATETIME", \
    "ScheduledDeparture DATETIME", \
    "ActualArrival DATETIME", \
    "ActualDeparture DATETIME" \
    ]
  Identity = [ \
    "TrainNum", \
    "OrigTime", \
    "StationCode" \
    ]
  Mapping = { \
    "StationCode": "code", \
    "TrainNum": "TrainNum", \
    "OrigTime": "OrigTime", \
    "ScheduledArrival": "adj_scharr", \
    "ScheduledDeparture": "adj_schdep", \
    "ActualArrival": "adj_postarr", \
    "ActualDeparture": "adj_postdep" \
    }

class Station(MySQLObject):
  TableName = "stations"
  Fields = [ \
    "Code", \
    "Name", \
    "Latitude", \
    "Longitude", \
    "Address", \
    "City", \
    "State", \
    "ZipCode", \
    "IsTrainSt", \
    "Type", \
    "DateModif" \
    ]
  # Interesting note, Canadian ZipCodes are non-numeric
  FieldDefinition = [ \
    "Code CHAR(3) PRIMARY KEY", \
    "Name TEXT", \
    "Latitude DOUBLE", \
    "Longitude DOUBLE", \
    "Address TEXT", \
    "City TEXT", \
    "State ENUM('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA'," + \
               "'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD'," + \
               "'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ'," + \
               "'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC'," + \
               "'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'," + \
               "'DC', 'AB', 'BC', 'ON', 'QC', '')", \
    "ZipCode CHAR(7)", \
    "IsTrainSt ENUM('Y', 'N')", \
    "Type ENUM('', 'Platform only (no shelter)', 'Platform with Shelter', 'Station Building (with waiting room)')", \
    "DateModif DATETIME" \
    ]
  Key = [ \
    "Code" \
    ]
  Identity = [ \
    "Code", \
    "DateModif" \
    ]
  Mapping = {
    "Code": "Code", \
    "Name": "Name", \
    "Latitude": "Latitude", \
    "Longitude": "Longitude", \
    "Address": "Address1", \
    "City": "City", \
    "State": "State", \
    "ZipCode": "Zipcode", \
    "IsTrainSt": "IsTrainSt", \
    "Type": "StaType", \
    "DateModif": "LastModif" \
    }

# I've seperated out aliases because things get nonsensical
# and inefficient if I don't
class Alias(MySQLObject):
  TableName = "aliases"
  Fields = [ \
    "TrainNum", \
    "OrigTime", \
    "Alias" \
    ]
  FieldDefinition = [ \
    "TrainNum INT", \
    "OrigTime DATETIME", \
    "Alias INT", \
    ]
  Mapping = { \
    "TrainNum": "TrainNum", \
    "OrigTime": "OrigTime", \
    "Alias": "Alias" \
    }

def readGoogleEngineAsset(asset_id):
  asset_url = "https://www.googleapis.com/mapsengine/v1/tables/" + \
    asset_id + \
    "/features?version=published&key=" + \
    environ.get("GOOGLE_ENGINE_KEY")# + \
  done = False
  readings = []
  url = asset_url
  pages = []
  while not done:
    f = urlopen(url)
    data = jsonload(f)
    f.close()
    if "nextPageToken" in data:
      url = asset_url + "&pageToken=" + \
        str(data["nextPageToken"])
    else:
      done = True
    pages.append(data)
  return pages

# Takes a bit of work to 

# Creates a datetime.date object with the Nth occurence of a given weekday
def findWeekdayInMonth(weekday, week, month, year):
  # The day must be in this range
  r = range((week - 1) * 7 + 1, week * 7 + 1)
  for day in r:
    aDay = date(year, month, day)
    if aDay.isoweekday() == weekday:
      return aDay

def daylightSavingInEffect(dt):
  # DLST starts second Sunday in March at 2 AM
  dlstStartDate = findWeekdayInMonth( \
    int(environ.get("DLST_START_WEEKDAY") or 7), \
    int(environ.get("DLST_START_WEEK") or 2), \
    int(environ.get("DLST_START_MONTH") or 3), \
    dt.year)
  dlstStartTime = daytime( \
    int(environ.get("DLST_START_HOUR") or 2), \
    int(environ.get("DLST_START_MINUTE") or 0))
  dlstStartDateTime = datetime.combine(dlstStartDate, dlstStartTime)
  
  # DLST ends first Sunday in November at 2 AM
  dlstEndDate = findWeekdayInMonth( \
    int(environ.get("DLST_END_WEEKDAY") or 7), \
    int(environ.get("DLST_END_WEEK") or 1), \
    int(environ.get("DLST_END_MONTH") or 11), \
    dt.year)
  dlstEndTime = daytime( \
    int(environ.get("DLST_END_HOUR") or 2),
    int(environ.get("DLST_END_MINUTE") or 0))
  dlstEndDateTime = datetime.combine(dlstEndDate, dlstEndTime)
  return dt > dlstStartDateTime and dt < dlstEndDateTime

def adjustToUTC(dt, tz):
  dt2 = dt
  # UTC never experiences daylightSavingTime
  if tz != 'U' and daylightSavingInEffect(dt):
    # Brings in line with standard time instead of daylight time
    dt2 = dt + timedelta(hours = -1)
  tzMap = { \
    'U': timedelta(hours = 0), \
    'E': timedelta(hours = -5), \
    'C': timedelta(hours = -6), \
    'M': timedelta(hours = -7), \
    'P': timedelta(hours = -8), \
    }
  return dt2 - tzMap[tz]

# Used in time stamps
def parseAmtrakDateTime(s, tz):
  # Account for timezones
  return adjustToUTC(datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p"), tz)

# Used in station schedules
def parseAmtrakDateTime2(s, tz):
  # Need to sort things out with the daylight savings time 
  # plus the associated Arizona problem
  return adjustToUTC(datetime.strptime(s, "%m/%d/%Y %H:%M:%S"), tz)

def decode_routes_page(uri):
  f = urlopen(uri);
  page = jsonload(f)
  f.close()
  return [page]

def decode_trains_asset(asset_id):
  print "=== TRAIN ASSETS ==="
  # TODO, not very robust if certain pieces of data are missing
  pages = readGoogleEngineAsset(asset_id)
  trains = MySQLObjectGroup(Train())
  readings = MySQLObjectGroup(TrainReading())
  stops = MySQLObjectGroup(TrainStop())
  aliases = MySQLObjectGroup(Alias())
  for page in pages:
    # Aliasing of train numbers throws a major wrench into this
    for feature in page.get("features"):
      geom = feature.get("geometry");
      coords = geom.get("coordinates")
      properties = feature.get("properties");
      properties["TrainNum"] = int(properties["TrainNum"])
      aliasNums = None
      # Add the adjusted origin time
      properties["OrigTime"] = parseAmtrakDateTime(properties.get("OrigSchDep"),
          properties.get("OriginTZ"))
      # append latitude and longitude to properties
      properties["Longitude"] = coords[0]
      properties["Latitude"] = coords[1]
      # Record the aliases
      aliasString = properties.get("Aliases")
      if aliasString != "":
        for alias in aliasString.split(","):
          properties["Alias"] = alias
          aliasNums = aliases.add(Alias(properties))
      trains.add(Train(properties))
      # append the adjusted reading time
      readingTime = parseAmtrakDateTime(properties.get("LastValTS"), \
          properties.get("EventTZ") or properties.get("OriginTZ"))
      #if readingTime.hour != readingTime.utcnow().hour:
      #  # There are weird cases where the time simply doesn't align
      #  # Rather, new readings need to be time-stamped
      #  print("RAW TIME")
      #  print(parseAmtrakDateTime(properties.get("LastValTS"), 'U'))
      #  print("EventTZ: " + str(properties.get("EventTZ")))
      #  print("OriginTZ: " + properties.get("OriginTZ"))
      #  print("ADJUSTED")
      #  print(readingTime)
      properties["RecordTime"] = readingTime
      readings.add(TrainReading(properties))
      count = 1
      while ("Station" + str(count)) in properties:
        stopinfo = jsonloads(properties.get("Station" + str(count)))
        stopinfo["TrainNum"] = properties.get("TrainNum")
        stopinfo["OrigTime"] = properties.get("OrigTime")
        station = stopinfo.get("code")
        scheddeptext = stopinfo.get("schdep")
        schedarrtext = stopinfo.get("scharr")
        actdeptext = stopinfo.get("postdep")
        actarrtext = stopinfo.get("postarr")
        timezone = stopinfo.get("tz")
        stopinfo["adj_schdep"] = scheddeptext and \
          parseAmtrakDateTime2(scheddeptext, timezone)
        stopinfo["adj_scharr"] = schedarrtext and \
          parseAmtrakDateTime2(schedarrtext, timezone)
        stopinfo["adj_postdep"] = actdeptext and \
          parseAmtrakDateTime2(actdeptext, timezone)
        stopinfo["adj_postarr"] = actarrtext and \
          parseAmtrakDateTime2(actarrtext, timezone)
        # Be aware of the use of boolean short-circuiting here
        stops.add(TrainStop(stopinfo))
        count += 1
  return {Train(): trains, \
          TrainReading(): readings, \
          TrainStop(): stops, \
          Alias(): aliases \
         }

def decode_stations_asset(asset_id):
  print "=== STATION ASSETS ==="
  pages = readGoogleEngineAsset(asset_id)
  stations = MySQLObjectGroup(Station())
  for page in pages:
    for feature in page.get("features"):
      # Convert the station data to objects
      geom = feature.get("geometry")
      properties = feature.get("properties")
      coords = geom.get("coordinates")
      properties["Longitude"] = coords[0]
      properties["Latitude"] = coords[1]
      # Needs to convert datetime
      # An issue is that I can't tell what timezone
      # those modifications are time stamped in
      # TODO: stop assuming UTC timezone for this case
      # TODO: find the method of finding the correct timezone
      properties["LastModif"] = parseAmtrakDateTime( \
        properties.get("DateModif"), 'U')
      stations.add(Station(properties)) \
  # Check the existing codes
  return stations

# within 0.005 degrees latitude/longitude (~500 meters) are the same position

# Used for decoding the route list json file
def as_route_list(json):
  # Creates a list with the lower case names of all routes
  # CMSIDs and ZoomLevel aren't very useful so I'm ignoring them
  ret = []
  for entries in json:
    # Names are mixed up too sometimes
    ret.extend(entries["Name"].split(" / "))
  return ret

# Used for decoding train assets
def as_route(json):
  ret = dict()
  for point in json["features"]:
    pass

def as_train_readings(json):
  ret = []
  for feature in json["features"]:
    ret.append(TrainReading(feature));
  return ret

# Allows using a very simple file for SSL configuration
def read_ssl_keys(fname):
  if fname == None:
    return None
  ssl_keys = []
  ssl_values = []
  with open(db_ssl_file, 'r') as f:
    for line in f:
      parts = line.split('=', 2)
      ssl_keys.append(parts[0])
      ssl_values.append(parts[1])
  return fromkeys(ssl_keys, ssl_values)

def read_train_assets(base_url):
  pass


def main(args):
  tables = [ \
    Train(), \
    TrainReading(), \
    TrainStop(), \
    Station(), \
    Alias() \
    ]
  # --- OPEN DATABASE ---
  # Uses environment variables
  # Opens a connection to the database
  # Uses environment variables to configure
  # Uses get to prevent KeyErrors
  db = connect( \
    host = environ.get('DB_HOST'), \
    user = environ.get('DB_USER'), \
    passwd = environ.get('DB_PASSWD'), \
    db = environ.get('DB_NAME'), \
    port = environ.get('DB_PORT') or 3306, \
    #unix_socket = environ.get('DB_UNIX_SOCKET') or "", \
    compress = environ.get('DB_COMPRESS') or False, \
    read_default_file = environ.get('DB_OPTION_FILE') or "", \
    ssl = read_ssl_keys(environ.get('DB_SSL_FILE')) \
    )
  existing = MySQLObject.getExistingTables(db)
  print(existing)
  # --- DROP THE TABLES ---
  if "--reset" in args:
    print("RESETING TABLES")
    for table in tables:
      table.destroy(db, existing)
    existing = MySQLObject.getExistingTables(db)
    db.commit()
  # --- CREATE TABLES ---
  # Used to prevent attempts to create pre-existing tables
  
  for table in tables:
    table.initialize(db, existing)
  db.commit()
  # --- ACCESS NEAR STATIC INFORMATION ---
  # Contains a list of train routes
  # Generally at http://www.amtrak.com/rttl/js/RoutesList.json
  # Note: RoutesList.json isn't really helpful so I won't download it
  #f = urlopen(environ.get('TRAIN_ROUTE_LIST_URI'))
  
  # Contains geometry for train routes
  # Generally at http://www.amtrak.com/rttl/js/route_properties.json
  route_pages = decode_routes_page(environ.get('TRAIN_ROUTE_PROPERTY_URI'))
  
  
  # TODO: Wishlist, gain access to the train data directly instead of from a hack
  
  # --- Configure Polling Rates ---
  poll_cycle_text = environ.get("POLL_CYCLE")
  station_poll_cycle_text = environ.get("STATION_POLL_CYCLE")
  
  if poll_cycle_text:
    poll_cycle = int(poll_cycle_text)
  else:
    # Updates every 5 minutes
    poll_cycle = 60 * 5
  
  if station_poll_cycle_text:
    station_poll_cycle = int(station_poll_cycle_text)
  else:
    # Updates the station list daily
    station_poll_cycle = 24 * 60 / poll_cycle
  
  # Uses file descriptor 3 to connect to write to the analysis program
  analysisPipe = fdopen(3, 'w')
  # --- POLL HIGHLY DYNAMIC INFORMATION ---
  running = True
  start_time = 0.0
  count = 0
  lastTS = None
  currentTS = None
  while running:
    if count == 0:
      stations = decode_stations_asset( \
        environ.get("GOOGLE_ENGINE_STATIONS_ASSET_ID"))
      oldstations = MySQLObjectGroup(Station())
      oldstations.read(db)
      newstations = stations - oldstations
      if len(newstations):
        newstations.write(db)
    # Time in seconds since the epoch
    current_time = time()
    # Number of seconds the previous run-through required
    time_diff = current_time - start_time
    start_time = current_time
    # If the time reset, just immediately request
    if time_diff < 0:
      time_diff = poll_cycle
    if time_diff < poll_cycle:
      print "sleep:        " + str(poll_cycle - time_diff)
      sleep(poll_cycle - time_diff)
    currentTS = datetime(1990, 1, 1).utcnow()
    # Record the start time
    start_time = time()
    # Read train assets
    train_data = decode_trains_asset(environ.get("GOOGLE_ENGINE_TRAINS_ASSET_ID"))
    for key, entry in train_data.iteritems():
      # There's a major issue now where data needs to update but still has its same identity
      # Fixing this issue may require following a much different paradigm, more similar to how analysis.R now is
      oldentry = MySQLObjectGroup(key)
      oldentry.read(db)
      newentry = entry - oldentry
      if key.TableName == "readings":
        if lastTS:
          print "LAST TIMESTAMP"
          print lastTS
          print "CURRENT TIMESTAMP"
          print currentTS
        for timeentry in newentry:
          entrytime = timeentry.data["Time"]
          if lastTS and entrytime.hour != currentTS.hour:
            print("CURRENT TIME")
            print(currentTS)
            print("STRANGE TIME")
            print(entrytime)
      newentry.write(db)
    count = (count + 1) % station_poll_cycle
    db.commit()
    # Write a character to the analysis pipe to trigger an update
    analysisPipe.write(' ')
    analysisPipe.flush()
    lastTS = currentTS
  analysisPipe.write('q')
  analysisPipe.close()
  db.close()

if __name__ == '__main__':
  main(argv)

