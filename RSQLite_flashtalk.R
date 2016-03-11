###############################################################################
###################           Using SQLite in R            ####################
###################              Daniel Park               ####################
###############################################################################

# Part 1: Shows how to create an SQLite database by directly downloading
#   zipped files.
#
# Part 2: Shows basics of using an RSQLite database

###############################################################################
###################                 PART 1                 ####################
###############################################################################

###############################################################################
# ATTENTION!!!
# Set working directory
# i.e. setwd("/Users/DanielPark/Documents/R_Projects/Airline")
###############################################################################

# Check to see if `RSQLite` package is installed
if (!"RSQLite" %in% installed.packages()){
  install.packages("RSQLite")
}

# Load `RSQLite`
library(RSQLite)

# `download.file()` will create a zipped file named `airline87_zipped.bz2`,
#    which will appear in your working directory.
# Data comes from "http://stat-computing.org/dataexpo/2009/the-data.html"
download.file(url="http://stat-computing.org/dataexpo/2009/1987.csv.bz2", 
              destfile="airline87_zipped.bz2")

# Allows for reading of a bzfile.
airline.bzfile <- bzfile(description="airline87_zipped.bz2")

# Create csv file which will be loaded into memory.
# May take a minute.
airline87 <- read.csv(file=airline.bzfile)

# Establishing a connection that will allow you to communicate 
#   with the database.
# Because there is no existing database, an empty database named 
#   `ontime.sqlite` will be created.
db.connection <- dbConnect(drv=SQLite(), dbname="ontime.sqlite")
# At the end of this R script, the connection will be closed.
# Among other reasons, disconnecting will free up memory.
# But now that an empty database has been created, running the same exact 
#   `dbConnect()` command will not create a new database named 
#   `ontime.sqlite`.  It will merely re-establish that connection.

# `dbListTables()` will tell you what tables (if any) are in your database.
dbListTables(conn=db.connection)

# Creating the basic structure for our table which will be in our database.
# Note that multiple tables can exist in a database.
# Table name is `ontime_tbl`
# Template based off of "http://stat-computing.org/dataexpo/2009/sqlite.html"
dbSendQuery(conn=db.connection,
            statement="CREATE TABLE ontime_tbl  
            (Year INTEGER,
            Month INTEGER,
            DayofMonth INTEGER,
            DayOfWeek INTEGER,
            DepTime  INTEGER,
            CRSDepTime INTEGER,
            ArrTime INTEGER,
            CRSArrTime INTEGER,
            UniqueCarrier VARCHAR(5),
            FlightNum INTEGER,
            TailNum VARCHAR(8),
            ActualElapsedTime INTEGER,
            CRSElapsedTime INTEGER,
            AirTime INTEGER,
            ArrDelay INTEGER,
            DepDelay INTEGER,
            Origin VARCHAR(3),
            Dest VARCHAR(3),
            Distance INTEGER,
            TaxiIn INTEGER,
            TaxiOut INTEGER,
            Cancelled INTEGER,
            CancellationCode VARCHAR(1),
            Diverted INTEGER,
            CarrierDelay INTEGER,
            WeatherDelay INTEGER,
            NASDelay INTEGER,
            SecurityDelay INTEGER,
            LateAircraftDelay INTEGER
            )") 

# Check to see if table has been created
dbListTables(conn=db.connection)
# `dbListFields()` will tell you the column names of a specified table
#   in your database.
dbListFields(conn=db.connection, name="ontime_tbl")


# Add csv file to your table in your database.
dbWriteTable(conn=db.connection, name='ontime_tbl', 
             value=airline87, append=TRUE, row.names=FALSE)

# Tells us how many rows in data.
# `rowid` is automatically generated when creating database.
dbGetQuery(conn=db.connection, 
           statement="SELECT rowid FROM ontime_tbl 
           ORDER BY rowid 
           DESC LIMIT 1;")

# Check results
# First 10
dbGetQuery(conn=db.connection, 
           statement="SELECT * FROM ontime_tbl 
           LIMIT 10;")

# Last 10
dbGetQuery(conn=db.connection, 
           statement="SELECT * FROM ontime_tbl 
           ORDER BY rowid 
           DESC LIMIT 10;")

# Delete downloaded file from working directory.
# No longer needed.
file.remove(file="airline87_zipped.bz2")

# Close connection.
dbDisconnect(conn=db.connection)

###################             END OF PART 1               ###################


###############################################################################
###################                 PART 2                 ####################
###############################################################################
# This is a lesson on using basic SQL commands in R.

###############################################################################
# Part 2A: Basics of setting up SQL environment in R

# Establish connection with your SQLite database.
db.connection <- dbConnect(drv=SQLite(), dbname="ontime.sqlite")

# If your database was not located in your working directory, 
#   you can specify a file path:
#db.connection <- dbConnect(drv=SQLite(), 
#                           dbname="~/Documents/East_Bay/DataScienceClub/ontime.sqlite")

# Basic query
dbGetQuery(conn=db.connection, 
           statement="SELECT ArrDelay FROM ontime_tbl LIMIT 10;")

# Average distance of flight
dbGetQuery(conn=db.connection, 
           statement="SELECT avg(Distance) FROM ontime_tbl;")

# `dbGetQuery()` returns a data frame
first10rows <- dbGetQuery(conn=db.connection, 
                          statement="SELECT * FROM ontime_tbl LIMIT 10;")
str(object=first10rows) # Examine structure


Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql()` 
  dbGetQuery(conn=db.connection, statement=sql.command)
}

# Simplified query
Sql("SELECT ArrDelay FROM ontime_tbl LIMIT 10;")

# Semicolon not necessary
Sql("SELECT ArrDelay FROM ontime_tbl LIMIT 10")

# When quotes are necessary in a query
Sql("SELECT COUNT(UniqueCarrier='UA') FROM ontime_tbl;")

###############################################################################
# Part 2B:  Using the `sqldf` package
# If you want to practice using SQL commands on a data frame

# Check to see if `sqldf` package is installed
if (!"sqldf" %in% installed.packages()){
  install.packages("sqldf")
}

# Load `sqlf` package
library(sqldf)

# Import 1000 rows from SQLite database
air.data <- Sql("SELECT * FROM ontime_tbl LIMIT 1000")

# Note: if object name contains a period, it must be contained in quotes.
sqldf("SELECT * FROM 'air.data' 
      LIMIT 10;")

# Close connection.
dbDisconnect(conn=db.connection)


###################             END OF PART 2               ###################
