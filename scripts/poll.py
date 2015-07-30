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

from json import load as jsonload, dumps as jsonsaves


class MySQLObject(object):
  TableName = ""
  Fields = []
  FieldDefinition = []
  FieldDefault = {}
  WriteFields = None
  ReadFields = None
  data = {}
  def __init__(self):
    if not self.WriteFields:
      self.WriteFields = self.Fields
    if not self.ReadFields:
      self.ReadFields = self.Fields
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
    dup.data = self.data.copy()
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

class MySQLObjectGroup(list):
  base = None
  def __init__(self, b):
    base = b
    super(MySQLObjectGroup, self).__init__()
  def write(self, db):
    cur = db.cursor()
    slots = base.subList(len(base.WriteFields))
    rep = [base.TableName]
    rep.extend(base.WriteFields)
    for entry in entries:
      rep.extend(entry.data)
    cur.execute( \
      "INSERT INTO %s (" + slots + ") VALUES " + \
      ", ".join(["(" + slots + ")"] * self.size()) + \
      ";", rep);
    cur.close()
    db.commit()
  def read(self, db):
    # Appends the results to the end of the existing list
    cur = db.cursor()
    rep = []
    rep.extend(base.ReadFields)
    rep.extend(base.TableName)
    cur.execute( \
      "SELECT " + ", ".join(["%s"] * len(base.ReadFields)) + \
      " FROM %s;", rep)
    for fetch in cur:
      entry = base.copy()
      idx = 0
      for field in ReadFields:
        entry.data[field] = entry[idx]
        idx += 1
      entries.append(entry)
    cur.close()
    def writeStream(self, f):
      # Writes the group of objects as JSON objects
      justdata = []
      for entry in entries:
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
    "ID", \
    "TrainNum", \
    "RouteID", \
    "OrigStationCode", \
    "DestStationCode" \
    ]
  FieldDefinition = [ \
    "ID INT", \
    "TrainNum INT", \
    "RouteID INT", \
    "OrigStationCode CHAR(3)", \
    "DestStationCode CHAR(3)", \
    "PRIMARY KEY(ID)" \
    ]

class TrainReading(MySQLObject):
  TableName = "readings"
  Fields = [ \
    "TrainID", \
    "Latitude", \
    "Longitude", \
    "Time", \
    "Speed", \
    "Heading", \
    "State"]
  FieldDefinition = [
    "TrainID INT", \
    "Latitude DOUBLE", \
    "Longitude DOUBLE", \
    "Time DATETIME", \
    "Speed DOUBLE", \
    "Heading ENUM('N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW')", \
    "State ENUM('Predeparture', 'Active', 'Completed')" \
    ]

class TrainStop(MySQLObject):
  TableName = "stops"
  Fields = [ \
    "StationCode", \
    "TrainID", \
    "OrigSchDep" \
    ]
  FieldDefinition = [ \
    "StationCode CHAR(3)", \
    "TrainID INT", \
    "OrigSchDep DATETIME" \
    ]

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
               "'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', '')", \
    "ZipCode INT", \
    "IsTrainSt BOOLEAN", \
    "Type ENUM('', 'Platform only (no shelter)', 'Platform with Shelter', 'Station Building (with waiting room)')", \
    "DateModif DATETIME" \
    ]
  def __init__(self, \
      code = "\0\0\0", 
      lonlat = (0.0, 0.0), 
      address = "", 
      city = "",
      state = "",
      zipCode = 0,
      isTrainStation = False,
      stationType = '',
      modified = datetime(1900, 1, 1)):
    self.data = {}
    self.data["Code"] = code;
    self.data["Longitude"] = lonlat[0];
    self.data["Latitude"] = lonlat[1];
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

def parseAmtrakDateTime(s):
  return datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p")

def decode_routes_page(uri):
  f = urlopen(uri);
  page = jsonload(f)
  return [page]

def decode_trains_asset(asset_id):
  print "=== TRAIN ASSETS ==="
  pages = readGoogleEngineAsset(asset_id)
  for page in pages:
    pass
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
      stations.append(Station( \
        properties.get("Code"), \
        geom.get("coordinates"), \
        properties.get("Address1"), \
        properties.get("City"), \
        properties.get("State"), \
        properties.get("ZipCode"), \
        properties.get("IsTrainSt") == 'Y', \
        properties.get("StaType"), \
        parseAmtrakDateTime(properties.get("DateModif")) \
        ))
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

