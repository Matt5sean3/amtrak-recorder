#!/usr/bin/python

from MySQLdb import connect
from os import mkdir
from os.path import isdir
from sys import stdout

import cgi
import cgitb
logdir = "/tmp/py_cgitb_log"
if not isdir(logdir):
  mkdir(logdir)
cgitb.enable(display=0, logdir=logdir)

from json import dumps as jsonsaves, dump as jsonsave

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

def route(db, args):
  cur = db.cursor()
  # Retrieve distinct route names from the database
  cur.execute("SELECT DISTINCT RouteName FROM trains;")
  ret = ServiceResult("application/json", jsonsaves([arr[0] for arr in cur.fetchall()]))
  cur.close()
  return ret

def train(db, args):
  cur = db.cursor()
  # Retrieve distinct train numbers from the database
  cur.execute("SELECT DISTINCT TrainNum FROM trains WHERE RouteName=%s", \
    args["route_name"])
  ret = ServiceResult("application/json", jsonsaves([arr[0] for arr in cur.fetchall()]))
  cur.close()
  return ret

def predict(db, args):
  cur = db.cursor()
  # Retrieve predictions for the given day
  pass

def source(db, args):
  cur = db.cursor()
  cur.execute("SELECT DISTINCT Source FROM departure_predictions;")
  ret = ServiceResult("application/json", jsonsaves([arr[0] for arr in cur.fetchall()]))
  cur.close()
  return ret

def main():
  services = { \
    "route": route, \
    "train": train, \
    "predict": predict, \
    "source": source
    }
  # This script pulls from the MySQL database
  # Should be served over https, otherwise passwords are 
  # available on the wire as plain text
  host = "localhost"
  db = "train_database"
  port = 3306
  # Retrieve user arguments
  # command
  # Possible values for command:
  # predict: Predicts arrival of all trains
  # list_routes: Lists the names of all the routes
  # list_trains: Lists the train 
  
  # user
  # password
  # source
  form = cgi.FieldStorage()
  if "user" in form:
    user = form.getfirst("user")
  else:
    user = "trains"

  if "pass" in form:
    passwd = form.getfirst("pass")
  else:
    passwd = "train spotter"

  db = connect( \
    host = host, \
    user = user, \
    passwd = passwd, \
    db = db, \
    port = port \
    )
  if "service" in form:
    result = services[form.getfirst("service")](db, form);
  else:
    result = ServiceResult("text/html", "<!DOCTYPE html><html><head><title>No Service Requested</title></head><body>Request must contain a service argument</body></html>")
  # For now expect CGI, but in the future use FastCGI
  out_pipe = stdout
  result.writeToFile(out_pipe)
  db.close()

if __name__ == "__main__":
  main()

