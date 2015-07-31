#!/usr/bin/python

# This script is meant to pull information from the Google Maps Engine
# used internally by the Track a Train Webapp
# Ideally, pulling directly from the source to generate the map will be 
# possible.

# Used to delay updates
from time import sleep
from time import time
from datetime import datetime

# Used to poll the data files
from urllib2 import urlopen

# Used to open numbered file descriptors to write to
# for use with ArcLaunch
from os import fdopen, environ

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
  data = {}
  def __init__(self):
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
    ax = 0
    for key in self.Identity:
      ax += hash(self.data[key])
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
    print \
      "CREATE TABLE " + self.TableName + \
      " (" + ", ".join(self.FieldDefinition) + ");"
    cur.execute( \
      "CREATE TABLE " + self.TableName + \
      " (" + ", ".join(self.FieldDefinition) + ");")
    cur.close()
    db.commit()
  
  def destroy(self, db):
    cur = db.cursor()
    cur.execute( \
      "DROP TABLE %s", (self.TableName,))
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
    cur = db.cursor()
    slots = self.base.subList(len(self.base.WriteFields))
    rep = []
    for entry in self:
      ordered = []
      for key in self.base.WriteFields:
        ordered.append(entry.data[key])
      rep.extend(ordered)
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

class Route(MySQLObject):
  TableName = "routes"
  name = ""
  Fields = [ \
    'ID', \
    'name' \
  ]
  WriteFields = [ \
    'name' \
  ]
  FieldDefinition = [ \
    'ID INT AUTO_INCREMENT', \
    'name TEXT', \
    'PRIMARY KEY(id)' \
  ]
  FieldDefault = {
    "ID": 0, \
    "name": ""
  }
  def __init__(self, n = ""):
    name = n
    super(Route, self).__init__();


class Segment(MySQLObject):
  TableName = "segments"
  Fields = [ \
    "RouteID", \
    "North", \
    "South", \
    "East", \
    "West" \
    ]
  FieldDefinition = [ \
    "RouteID INT", \
    "North DOUBLE", \
    "South DOUBLE", \
    "East DOUBLE", \
    "West DOUBLE" \
    ]

class Train(MySQLObject):
  TableName = "trains"
  Fields = [ \
    "TrainNum", \
    "RouteID", \
    "OrigStationCode", \
    "DestStationCode" \
    ]
  FieldDefinition = [ \
    "TrainNum INT PRIMARY KEY", \
    "RouteID INT", \
    "OrigStationCode CHAR(3)", \
    "DestStationCode CHAR(3)" \
    ]

class TrainReading(MySQLObject):
  TableName = "readings"
  Fields = [ \
    "TrainNum", \
    "Latitude", \
    "Longitude", \
    "Time", \
    "Speed", \
    "Heading", \
    "State"]
  FieldDefinition = [
    "TrainNum INT", \
    "Latitude DOUBLE", \
    "Longitude DOUBLE", \
    "Time DATETIME", \
    "Speed DOUBLE", \
    "Heading ENUM('', 'N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW')", \
    "State ENUM('', 'Predeparture', 'Active', 'Completed')" \
    ]
  def __init__(self, \
      trainnum = 0, \
      lonlat = [0.0, 0.0], \
      time = datetime('1900', '1', '1'), \
      speed = 0.0, \
      heading = '', \
      state = '' \
      ):
    self.data["TrainNum"] = trainnum
    self.data["Longitude"] = lonlat[0]
    self.data["Latitude"] = lonlat[1]
    self.data["Time"] = time
    self.data["Speed"] = speed
    self.data["Heading"] = heading
    self.data["State"] = state
    super(TrainReading, self).__init__()

class TrainStop(MySQLObject):
  TableName = "stops"
  Fields = [ \
    "StationCode", \
    "TrainNum", \
    "ScheduledDeparture" \
    "ScheduledArrival" \
    ]
  FieldDefinition = [ \
    "StationCode CHAR(3)", \
    "TrainNum INT", \
    "ScheduledDeparture DATETIME" \
    "ScheduledArrival DATETIME" \
    ]
  def __init__(self, \
      stationcode = '', \
      trainnum = 0, \
      departure = datetime('1900', '1', '1'), \
      arrival = datetime('1900', '1', '1') \
      ):
    self.data["StationCode"] = stationcode
    self.data["TrainNum"] = trainnum
    self.data["ScheduledDeparture"] = departure
    self.data["ScheduledArrival"] = arrival
    super(TrainStop, self).__init__()

class TrainArrival(MySQLObject):
  TableName = "arrivals"
  Fields = [ \
    "TrainNum", \
    "StationCode", \
    "Time"]
  FieldDefinition = [ \
    "TrainNum INT", \
    "StationCode CHAR(3)", \
    "Time DATETIME"]
  def __init__(self, \
      trainnum = 0, \
      stationcode = '', \
      time = datetime('1900', '1', '1'), \
      ):
    self.data["TrainNum"] = trainnum
    self.data["StationCode"] = stationcode
    self.data["Time"] = time
    super(TrainDeparture, self).__init__()

class TrainDeparture(MySQLObject):
  TableName = "departures"
  Fields = [ \
    "TrainNum", \
    "StationCode", \
    "Time"]
  FieldDefinition = [ \
    "TrainNum INT", \
    "StationCode CHAR(3)", \
    "Time DATETIME"]
  def __init__(self, \
      trainnum = 0, \
      stationcode = '', \
      time = datetime('1900', '1', '1'), \
      ):
    self.data["TrainNum"] = trainnum
    self.data["StationCode"] = stationcode
    self.data["Time"] = time
    super(TrainDeparture, self).__init__()

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
    "IsTrainSt BOOLEAN", \
    "Type ENUM('', 'Platform only (no shelter)', 'Platform with Shelter', 'Station Building (with waiting room)')", \
    "DateModif DATETIME" \
    ]
  Key = [
    "Code"
    ]
  Identity = [
    "Code",
    "DateModif"
    ]
  def __init__(self, \
      code = "\0\0\0", 
      name = "",
      lonlat = (0.0, 0.0), 
      address = "", 
      city = "",
      state = "",
      zipCode = 0,
      isTrainStation = False,
      stationType = '',
      modified = datetime(1900, 1, 1)):
    super(Station, self).__init__()
    self.data = {}
    self.data["Code"] = code
    self.data["Name"] = name
    self.data["Longitude"] = lonlat[0]
    self.data["Latitude"] = lonlat[1]
    self.data["Address"] = address
    self.data["City"] = city
    self.data["State"] = state
    self.data["ZipCode"] = zipCode
    self.data["IsTrainSt"] = isTrainStation
    self.data["Type"] = stationType
    self.data["DateModif"] = modified


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
    if "nextPageToken" in data:
      url = asset_url + "&pageToken=" + \
        str(data["nextPageToken"])
    else:
      done = True
    pages.append(data)
  return pages

# Used in time stamps
def parseAmtrakDateTime(s):
  return datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p")

# Used in station schedules
def parseAmtrakDateTime2(s):
  return datetime.strptime(s, "%d/%m/%Y %H:%M:%S")

def decode_routes_page(uri):
  f = urlopen(uri);
  page = jsonload(f)
  return [page]

def decode_trains_asset(asset_id):
  print "=== TRAIN ASSETS ==="
  pages = readGoogleEngineAsset(asset_id)
  readings = MySQLObjectGroup(TrainReading())
  stops = MySQLObjectGroup(TrainStop())
  arrivals = MySQLObjectGroup(TrainArrival())
  departures = MySQLObjectGroup(TrainDeparture())
  for page in pages:
    for feature in page.get("features"):
      geom = feature.get("geometry");
      properties = feature.get("properties");
      readings.add(TrainReading( \
        trainnum = properties.get("TrainNum"), \
        lonlat = feature.get("geometry"), \
        time = parseAmtrakDateTime(properties.get("LastValTS"))), \
        speed = properties.get("Velocity"), \
        heading = properties.get("Heading"), \
        state = properties.get("TrainState") \
        ))
      count = 1
      while ("Station" + str(count)) in properties:
        stopinfo = jsonloads(properties.get("Station"))
        stops.add(TrainStop())
        count += 1
      
  return pages

def decode_stations_asset(asset_id):
  print "=== STATION ASSETS ==="
  pages = readGoogleEngineAsset(asset_id)
  stations = MySQLObjectGroup(Station())
  for page in pages:
    for feature in page.get("features"):
      # Convert the station data to objects
      geom = feature.get("geometry")
      point = geom.get("coordinates")
      properties = feature.get("properties")
      # Needs to convert datetime
      stations.add(Station( \
        properties.get("Code"), \
        properties.get("Name"),
        geom.get("coordinates"), \
        properties.get("Address1"), \
        properties.get("City"), \
        properties.get("State"), \
        properties.get("Zipcode"), \
        properties.get("IsTrainSt") == 'Y', \
        properties.get("StaType"), \
        parseAmtrakDateTime(properties.get("DateModif")) \
        ))
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


def main():
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
  # --- CREATE TABLES ---
  # Used to prevent attempts to create pre-existing tables
  existing = MySQLObject.getExistingTables(db)
  print(existing)
  Route().initialize(db, existing)
  Segment().initialize(db, existing)
  Train().initialize(db, existing)
  TrainReading().initialize(db, existing)
  TrainStop().initialize(db, existing)
  Station().initialize(db, existing)
  
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
  
  # --- POLL HIGHLY DYNAMIC INFORMATION ---
  running = True
  start_time = 0.0
  count = 0
  while running:
    if count == 0:
      stations = decode_stations_asset( \
        environ.get("GOOGLE_ENGINE_STATIONS_ASSET_ID"))
      oldstations = MySQLObjectGroup(Station())
      oldstations.read(db)
      newstations = stations - oldstations
      print(newstations)
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
    # Record the start time
    start_time = time()
    # Read train assets
    train_pages = decode_trains_asset(environ.get("GOOGLE_ENGINE_TRAINS_ASSET_ID"))
    count = (count + 1) % station_poll_cycle
    db.commit()

if __name__ == '__main__':
  main()

