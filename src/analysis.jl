#!/usr/bin/julia

using DBI
using MySQL
using Dates

# Needs to getenv

host = get(ENV, "DB_HOST", "localhost")
user = get(ENV, "DB_USER", "")
passwd = get(ENV, "DB_PASSWD", "")
dbname = get(ENV, "DB_NAME", "")
port = parse(get(ENV, "DB_PORT", "3306"))
unix_socket = get(ENV, "DB_UNIX_SOCKET", C_NULL)
client_flag = 0


db = connect(MySQL5, host, user, passwd, dbname, port, unix_socket, client_flag)

# The analysis is very limited for the time being:
# The distribution of times required for departing from one station and 
# arriving at the next station.
#
# The process is performed by looking at the schedule for all train numbers
# 

type Stop
  station::String
  arrival::DateTime
  departure::DateTime
end

type TrainRun
  TrainNum::Int
  schedule::Tuple{Stop}
  actual::Tuple{Stop}
end

# Generates a schedule object
function read_schedules()
end

# Start by getting the list of trains
get_trains_query = "SELECT TrainNum, OrigTime FROM trains;"

get_trains = prepare(db, get_trains_query)

execute(get_trains)

disconnect(db)

