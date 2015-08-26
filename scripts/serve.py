#!/usr/bin/python

from MySQLdb import connect

from json import dumps as jsonsaves, dump as jsonsave

def route_names(db, out_pipe, args):
  cur = db.cursor()
  # Retrieve distinct route names from the database
  cur.execute("SELECT DISTINCT RouteName FROM trains;")
  print("Content-type: application/json", "", "", \
    sep = "\r\n", end = "", file = out_pipe)
  jsonsave(cur.fetchall(), out_pipe)
  cur.close()

def train_nums(db, out_pipe, args):
  cur = db.cursor()
  # Retrieve distinct train numbers from the database
  cur.execute("SELECT DISTINCT TrainNum FROM trains WHERE RouteName=%s", \
    args["route_name"])
  jsonsave(cur.fetchall(), out_pipe)
  cur.close()

def predict(db, out_pipe, args):
  cur = db.cursor()
  # Retrieve predictions for the given day
  pass

def main():
  cmds = { \
    "list_routes": route_names, \
    "list_trains": train_nums, \
    "predict": predict \
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
  db = connect( \
    host = host, \
    user = "", \
    db = db, \
    port = port \
    )
  # For now expect CGI, but in the future use FastCGI
  out_pipe = sys.stdout
  # Print the header
  # Print the data as JSON
  jsonsave(cur.fetchall(), out_pipe)
  cur.close()
  db.close()

if __name__ == "__main__":
  main()

