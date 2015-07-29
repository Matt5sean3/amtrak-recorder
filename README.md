# Amtrak Recorder
Pulls information from Amtrak's Google Engine and records it in an SQL database for later analysis. Will most certainly go out of date January 2016 when Google Maps Engine is discontinued unless the Track a Train WebApp on Amtrak's website is updated or Amtrak makes a more direct public API for train GPS signals available.

Note that for the moment the script probably isn't ideally designed. This is partly due to an expectation for the script to go out of date completely with the possibility that there will be no replacement for the Google Maps Engine and that the Track a Train WebApp may simply be discontinued when the Google Maps Engine is discontinued.

## Configuration and Running

A MySQL server with a user with permissions granted on at least one database must be available. Set the variables listed in "Environment Variables" accordingly.

The Python module MySQLdb must also be installed.

Before running this you need to either set a large number of environment variables or run the script using ArcLaunch.

## Environment Variables

A sizable list of environment variables are utilized to configure the polling script. These variables are described below:

### TRAIN\_ROUTE\_LIST\_URI

This is the URI of a JSON file containing the names of all the Amtrak routes.

### TRAIN\_ROUTE\_PROPERTY\_URI

This is the URI of a JSON file containing information about the routes. It should be essentially static in most cases.

### GOOGLE\_ENGINE\_TRAINS\_ASSET\_ID

This is the google engine asset id for the trains layer of the Track a Train WebApp. It is the most interesting of the assets as it is the most dynamic layer and is the source for train positions over time.

### GOOGLE\_ENGINE\_ROUTES\_ASSET\_ID

This is the google engine asset id for the routes layer of the Track a Train WebApp.

### GOOGLE\_ENGINE\_STAIONS\_ASSET\_ID

This is the google engine asset id for the stations layer of the Track a Train WebApp.

### GOOGLE\_ENGINE\_KEY

This is the google engine key from the Track a Train WebApp. It's required for accessing assets from the Google Maps Engine.

### DB\_HOST

The IP address of the MySQL server where the retrieved information will be stored.

### DB\_USER

The username of the user used to login to the MySQL server.

### DB\_PASSWD

The password of the user used to login to the MySQL server.

### DB\_NAME

The name of the database to store the recorded information in. This is different from the name of the tables in which the information is stored, which is not configurable at this time.


