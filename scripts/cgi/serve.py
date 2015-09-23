#!/usr/bin/python

from MySQLdb import connect, DatabaseError
from os import mkdir
from os.path import isdir
from sys import stdout

import cgi
import cgitb

#logdir = "/tmp/py_cgitb_log"
#if not isdir(logdir):
#  mkdir(logdir)
#cgitb.enable(display=0, logdir=logdir)
cgitb.enable()

from json import dumps as jsonsaves, dump as jsonsave, load as jsonload

class ServiceResult(object):
  def __init__(self, resType, resContent):
    self.mime = resType
    self.text = resContent
  def mimeType(self):
    return self.mime
  def content(self):
    return self.text
  def writeToFile(self, out):
    out.write("Content-type: " + self.mimeType() + "\r\n");
    out.write("Content-length: " + str(len(self.content())) + "\r\n\r\n");
    out.write(self.content())

def route(args, conf):
  
  cur = db.cursor()
  # Retrieve distinct route names from the database
  cur.execute("SELECT DISTINCT RouteName FROM trains;")
  ret = ServiceResult("application/json", jsonsaves([arr[0] for arr in cur.fetchall()]))
  cur.close()
  return ret

def train(args, conf):
  db = connect( \
    host = conf["TRAIN_DB_HOST"], \
    user = form.getfirst("user"), \
    passwd = form.getfirst("pass"), \
    db = conf["TRAIN_DATABASE"], \
    port = conf["TRAIN_DB_PORT"] \
    )
  cur = db.cursor()
  # Retrieve distinct train numbers from the database
  cur.execute("SELECT DISTINCT `Number` FROM " + \
    conf["TRAIN_TABLE"] + " WHERE RouteName=%s", \
    args.getfirst("route_name"))
  ret = ServiceResult("application/json", jsonsaves([arr[0] for arr in cur.fetchall()]))
  cur.close()
  db.close()
  return ret

def predict(args, conf):
  cur = db.cursor()
  # Retrieve predictions for the given day
  pass

def source(args, conf):
  if "user" not in args or "pass" not in args:
    # return an error result
    pass
  db = connect( \
    host = conf["TRAIN_DB_HOST"], \
    user = args.getfirst("user"), \
    passwd = args.getfirst("pass"), \
    db = conf["TRAIN_DATABASE"], \
    port = conf["TRAIN_DB_PORT"] \
    )
  cur = db.cursor()
  cur.execute("SELECT DISTINCT Source FROM `" + \
    conf["DEPARTURE_TABLE"] + "`;")
  ret = ServiceResult("application/json", jsonsaves([arr[0] for arr in cur.fetchall()]))
  cur.close()
  db.close()
  return ret

def register(args, conf):
  # Check that sufficient arguments are available
  if "user" not in args or "pass" not in args:
    return ServiceResult("application/json", jsonsaves({"state": "failure", "message": "Need a username and password"}))
  db = connect( \
    host = conf["TRAIN_DB_HOST"], \
    user = conf["GATE_KEEPER_USERNAME"], \
    passwd = conf["GATE_KEEPER_PASSWD"], \
    db = conf["USER_DATABASE"], \
    port = conf["TRAIN_DB_PORT"] \
    )
  username = args.getfirst("user")
  passwd = args.getfirst("pass")
  cur = db.cursor()
  # Check that the user doesn't already exist
  cur.execute("SELECT DISTINCT user FROM mysql.user WHERE user=%s", \
    username)
  if cur.fetchone() is not None:
    cur.close()
    db.close()
    return ServiceResult("application/json", jsonsaves({"success": False, \
      "message": "User already exists"}))
  cur.execute("CREATE USER %s@%s IDENTIFIED BY %s", (username, \
    conf["TRAIN_DB_WEB_HOST"], passwd))
  # duplicate backticks for the table name
  table_name = username.replace("`", "``") + "_data"
  cur.execute("CREATE TABLE `" + table_name + \
    "` (Latitude DOUBLE, Longitude DOUBLE, Time DATETIME);")
  cur.execute("GRANT INSERT, DROP ON `" + table_name + "` TO %s@%s", \
    (username, conf["TRAIN_DB_WEB_HOST"]))
  cur.execute("GRANT SELECT ON `" + conf["TRAIN_DATABASE"] + "`.* TO %s@%s", \
    (username, conf["TRAIN_DB_WEB_HOST"]))
  cur.close()
  db.close()
  return ServiceResult("application/json", \
    jsonsaves({"success": True, "message": "Created new user"}))

def deregister(args, conf):
  # Drop the user's data table from the user's account
  username = args.getfirst("user")
  passwd = args.getfirst("pass")
  db = connect( \
    host = conf["TRAIN_DB_HOST"], \
    user = username, \
    passwd = passwd, \
    db = conf["USER_DATABASE"], \
    port = conf["TRAIN_DB_PORT"] \
    )
  cur = db.cursor()
  table_name = username.replace("`", "``") + "_data"
  cur.execute("DROP TABLE `" + table_name + "`;")
  cur.close()
  db.close()
  # Connect with a different account for dropping the user
  db = connect( \
    host = conf["TRAIN_DB_HOST"], \
    user = conf["GATE_KEEPER_USERNAME"], \
    passwd = conf["GATE_KEEPER_PASSWD"], \
    db = conf["USER_DATABASE"], \
    port = conf["TRAIN_DB_PORT"] \
    )
  cur = db.cursor()
  db.close()
  return ServiceResult("application/json", \
    jsonsaves({"success": True, "message": "Dropped user from system"}))

def validate(args, conf):
  username = args.getfirst("user")
  passwd = args.getfirst("pass")
  try:
    db = connect( \
      host = conf["TRAIN_DB_HOST"], \
      user = username, \
      passwd = passwd, \
      db = conf["USER_DATABASE"], \
      port = conf["TRAIN_DB_PORT"] \
      )
    db.close()
    return ServiceResult("application/json", \
      jsonsaves({"value": True, "message": "This user exists"}))
  except DatabaseError:
    return ServiceResult("application/json", \
      jsonsaves({"value": False, "message": "This user does not exist"}))

def main():
  f1 = open("conf_path", "r")
  conf_path = f1.readline()
  f1.close()
  # Retrieve the configuration file
  f = open(conf_path, "r")
  conf = jsonload(f)
  f.close()
  services = { \
    "route": route, \
    "train": train, \
    "predict": predict, \
    "source": source, \
    "register": register, \
    "deregister": deregister, \
    "validate": validate \
    }
  # This script pulls from the MySQL database
  # Should be served over https, otherwise passwords are 
  # available on the wire as plain text
  form = cgi.FieldStorage()
  if "service" in form:
    result = services[form.getfirst("service")](form, conf);
  else:
    result = ServiceResult("text/html", "<!DOCTYPE html><html><head><title>Failed Request</title></head><body>Request must contain a service argument</body></html>")
  # For now expect CGI, but in the future use FastCGI
  out_pipe = stdout
  result.writeToFile(out_pipe)

if __name__ == "__main__":
  main()

