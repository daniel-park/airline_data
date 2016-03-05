# Part 1: Shows how to create an SQLite database by directly downloading
#   zipped files.
#
# Part 2: Shows basics of using an RSQLite database

###############################################################################
###################                 PART 1                 ####################
###############################################################################

###############################################################################
# ATTENTION!!!
# This is the part you need to change.
# Set working directory
# i.e. setwd("/Users/DanielPark/Documents/R_Projects/DataSciAirline")
###############################################################################

# Check to see if `RSQLite` package is installed
if (!"RSQLite" %in% installed.packages()){
  install.packages("RSQLite")
}

# Load `RSQLite`
library(RSQLite)



# Establishing a connection that will allow you to communicate 
#   with the database.
# Because there is no existing database, an empty database named 
#   `airline88.sqlite` will be created.
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






# `airline_file` is the designated file name given to the downloaded file.
# The downloaded file will be saved in your working directory.
airline.file <- "airline88_zipped"

# `airline.url` is the specific web url where the zipped file is located.
airline.url <- "http://stat-computing.org/dataexpo/2009/1988.csv.bz2"

# Download file
# Recall that the variables `airline.url` and `airline.file` were 
#   created above.
# `download.file()` will generate the csv file named `airline88.csv`,
#    which will appear in your working directory.
download.file(url=airline.url, destfile=airline.file)

# Allows for reading of a bzfile.
airline.bzfile <- bzfile(description=airline.file)

# Create csv file which will be loaded into memory.
# May take a few minutes.
airline88 <- read.csv(file=airline.bzfile)
# Using `system.time()`:
#    user  system elapsed 
# 173.803   4.355 215.931 

# Remove downloaded file from working directory
file.remove(file="airline88_zipped")

# Add csv file to your table in your database.
dbWriteTable(conn=db.connection, name='ontime_tbl', 
             value=airline88, append=TRUE, row.names=FALSE)

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

# Close connection.
dbDisconnect(db.connection)

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
  # Note dplyr package contains a function `sql` 
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

# Note: if field name (column name) contains a period, it must be contained in
#   quotes.
sqldf("SELECT * FROM 'air.data' 
      LIMIT 10;")

# Close connection.
dbDisconnect(conn=db.connection)


###################             END OF PART 2               ###################
