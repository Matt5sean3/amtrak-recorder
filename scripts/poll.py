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

# Updates every 5 minutes
trainDelaySecs = 60 * 5

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
      self.WriteFields = Fields
    if not self.ReadFields:
      self.ReadFields = Fields
  def subList(self, num):
    return ", ".join(["%s"] * num)
  def getDataList(self, names):
    ret = []
    for name in names:
      ret.append(self.data.get(name))
    return ret
  def initialize(self, db):
    cur = db.cursor()
    rep = [self.TableName]
    rep.extend(self.FieldDef)
    cur.execute( \
      "CREATE TABLE %s (" + subList(len(self.FieldDefinition)) + ");", \
      rep)
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

class MySQLObjectGroup(object):
  base = None
  entries = []
  def __init__(self, b):
    base = b
    entries = [b]
  def __len__(self):
    return len(entries)
  def get(self, idx):
    return entries.get(idx)
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
  def __init__(self, n):
    name = n
    super(Route, self).__init__(self);

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
  def __init__(self, routes, r = "", n = 0.0, s = 0.0, e = 0.0, w = 0.0):
    self.data["route"] = r;
    self.data["north"] = n;
    self.data["south"] = s;
    self.data["east"] = e;
    self.data["west"] = w;
    super(Segment, self).__init__(self);

class Train(MySQLObject):
  TableName = "trains"
  Fields = [ \
    "ID", \
    "TrainNum", \
    "RouteID", \
    "OrigStation", \
    "DestStation" \
    ]
  FieldDefinition = [ \
    "ID INT", \
    "TrainNum INT", \
    "RouteID INT", \
    "OrigStation CHAR(3)", \
    "DestStation CHAR(3)", \
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
    "StationID", \
    "TrainID", \
    "OrigSchDep" \
    ]
  FieldDefinition = [ \
    "StationID INT", \
    "TrainID INT", \
    "OrigSchDep DATETIME" \
    ]

class Station(MySQLObject):
  TableName = "stations"
  Fields = [ \
    "ID", \
    "Name", \
    "Code", \
    "Latitude", \
    "Longitude" \
    ]
  FieldDefinition = [ \
    "ID INT AUTO_INCREMENT", \
    "Name TEXT", \
    "Code CHAR(3)", \
    "Latitude DOUBLE", \
    "Longitude DOUBLE"
    ]

def parseAmtrakDateTime(s):
  return datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p")

class TrainReading(MySQLObject):
  data = None
  Fields = [ \
    "Latitude", \
    "Longitude", \
    "ID", \
    "TrainNum", \
    "Aliases", \
    "OrigSchDep", \
    "OriginTZ", \
    "TrainState", \
    "Velocity", \
    "RouteName", \
    "CMSID", \
    "OrigCode", \
    "DestCode", \
    "EventCode", \
    "EventDT", \
    "EventT", \
    "EventTZ", \
    "LastValTS", \
    "Heading"
    ]
  FieldTypeDefinition = [
    "ID INT", \
    "Latitude DOUBLE", \
    "Longitude DOUBLE", \
    "TrainNum INT UNSIGNED", \
    "Aliases TEXT", \
    "OrigSchDep DATETIME", \
    "OriginTZ CHAR", \
    "TrainState ENUM('Predeparture', 'Active', 'Completed')", \
    "Velocity DOUBLE", \
    "RouteName TEXT", \
    "CMSID BIGINT", \
    "OrigCode CHAR(3)", \
    "DestCode CHAR(3)", \
    "EventCode CHAR(3)", \
    "EventDT DATETIME", \
    "EventT ENUM('Update', 'Estimated Arrival')", \
    "EventTZ CHAR(1)", \
    "LastValTS DATETIME", \
    "Heading ENUM('N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW')"
    ]
  FieldDefaults = {
    "ID": 0,
    "Latitude": 0,
    "Longitude": 0,
    "TrainNum": 0,
    "Aliases": "",
    "OrigSchDep": "01/01/1900 12:00:00 AM",
    "OriginTZ": chr(0),
    "TrainState": "",
    "Velocity": 0,
    "RouteName": "",
    "CMSID": 0,
    "OrigCode": "",
    "DestCode": "",
    "EventCode": "",
    "EventDT": "01/01/1900 12:00:00 AM",
    "EventT": "Update",
    "EventTZ": chr(0),
    "LastValTS": "01/01/1900 12:00:00 AM",
    "Heading": ""
    }
  FieldConversions = {
    "OrigSchDep": parseAmtrakDateTime,
    "EventDT": parseAmtrakDateTime,
    "LastValTS": parseAmtrakDateTime
    }
  def __init__(self, source):
    self.data = source["properties"].copy();
    self.data["Longitude"] = source["geometry"]["coordinates"][0];
    self.data["Latitude"] = source["geometry"]["coordinates"][1];
    for entry, value in TrainReading.FieldDefaults.iteritems():
      if not entry in self.data or self.data[entry] == '':
        self.data[entry] = value
    for entry, value in TrainReading.FieldConversions.iteritems():
      self.data[entry] = value(self.data[entry])
  def getList(self, order):
    ret = []
    for item in order:
      entry = self.data.get(item) or TrainReading.FieldDefaults.get(item)
      ret.append(entry)
    return ret
  def record(self, cur):
    # Uses a provided database cursor to write
    # Record to the train_reading table
    cur.execute("INSERT INTO train_reading (" + \
      ", ".join(TrainReading.Fields) + \
      ") VALUES (" + \
      ", ".join(["%s"] * len(TrainReading.Fields)) + \
      ");", getList(TrainReading.Fields))
  @staticmethod
  def recordList(cur, readings):
    data = []
    for reading in readings:
      data.extend(reading.getList(TrainReading.Fields))
    # Splat operator, love the name
    cur.execute("INSERT INTO train_reading (" + \
      ", ".join(TrainReading.Fields) + \
      ") VALUES " + \
      ", ".join(["(" + ", ".join(["%s"] * len(TrainReading.Fields)) + ")"] * \
      len(readings)) + \
      ";", data)
  @staticmethod
  def initializeTable(cur):
    cur.execute("CREATE TABLE IF NOT EXISTS train_reading (" +
      ','.join(TrainReading.FieldTypeDef) + ");")

# within 0.005 degrees latitude/longitude (~500 meters) are the same position

def as_route_segment_dict(json):
  # Creates a dict with geometry for all routes
  # Names are always forced to lower case
  segments = dict()
  for entry in json["features"]:
    properties = entry["properties"]
    names = properties["NAME"].split(" / ")
    for name in names:
      lowerName = name.lower()
      segment = RouteSegment( \
        properties["NORTH"], \
        properties["SOUTH"], \
        properties["EAST"], \
        properties["WEST"])
      if lowerName in ret:
        segments[lowerName].append(segment)
      else:
        segments[lowerName] = [segment]
  return segments

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
  done = False
  readings = []
  url = base_url
  while not done:
    f = urlopen(url)
    data = jsonload(f)
    readings += as_train_readings(data)
    print "token:        " + str(data.get("nextPageToken"))
    if "nextPageToken" in data:
      url = base_url + "&nextPageToken=" + \
        data["nextPageToken"]
    else:
      done = True
  return readings

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
  # --- CREATE TABLE ---
  cur = db.cursor()
  TrainReading.initializeTable(cur)
  
  # --- ACCESS NEAR STATIC INFORMATION ---
  # Contains CMS information for accessing train routes
  # Generally at http://www.amtrak.com/rttl/js/RoutesList.json
  route_list = None
  f = urlopen(environ.get('TRAIN_ROUTE_LIST_URI'))
  route_list = jsonload(f)
  
  # Contains geometry for train routes
  # Generally at http://www.amtrak.com/rttl/js/route_properties.json
  route_paths = None
  f = urlopen(environ.get('TRAIN_ROUTE_PROPERTY_URI'))
  route_paths = jsonload(f)
  
  # TODO: gain access to the train data directly instead of from a hack
  
  # --- POLL HIGHLY DYNAMIC INFORMATION ---
  running = True
  train_asset_url = "https://www.googleapis.com/mapsengine/v1/tables/" + \
    environ.get("GOOGLE_ENGINE_TRAINS_ASSET_ID") + \
    "/features?version=published&key=" + \
    environ.get("GOOGLE_ENGINE_KEY")# + \
#    "&select=geometry," + \
#    ','.join(TrainReading.Fields[2, len(TrainReading.Fields)])
  start_time = 0.0
  while running:
    # Time in seconds since the epoch
    current_time = time()
    # Number of seconds the previous run-through required
    time_diff = current_time - start_time
    start_time = current_time
    # If the time reset, just immediately request
    if time_diff < 0:
      time_diff = trainDelaySecs
    if time_diff < trainDelaySecs:
      print "sleep:        " + str(trainDelaySecs - time_diff)
      sleep(trainDelaySecs - time_diff)
    # Record the start time
    start_time = time()
    # Read train assets
    trains = read_train_assets(train_asset_url)
    print "Num trains:     " + str(len(trains))
    TrainReading.recordList(cur, trains)
    db.commit()

if __name__ == '__main__':
  main()

