# The following shows how to create SQLite database by directly downloading
#   bz2 files.

###############################################################################
# ATTENTION!!!
# This is the part you need to change.
# Set working directory
setwd("/Users/DanielPark/Documents/R_Projects/DataSciAirline/airline_csv")
###############################################################################

# Check to see if `RSQLite` package is installed
if (!"RSQLite" %in% installed.packages()){
  install.packages("RSQLite")
}

# Load `RSQLite`
library(RSQLite)

# `airline_file` is the designated file name given to the downloaded file.
# The downloaded file will be saved in your working directory.
airline.file <- "airline88.csv"

# `airline.url` is the specific web url where the zipped file is located.
airline.url <- "http://stat-computing.org/dataexpo/2009/1988.csv.bz2"

# Establishing a connection that will allow you to communicate 
#   with the database.
# Because there is no existing database, an empty database named 
#   `airline88.sqlite` will be created.
db.connection <- dbConnect(drv=SQLite(), dbname="ontime.sqlite")
# At the end of this R script, the connection will be closed.
# Among other reasons, disconnecting will free up memory.
# But now that an empty database has been created, running the same exact 
#   `dbConnect()` command will not create a new database named 
#   `airline88.sqlite`.  It will merely re-establish that connection.

# Creating the basic structure for our table which will be in our database.
# Note that multiple tables can exist in a database.
# Table name is `ontime`
dbSendQuery(conn = db.connection,
            statement="CREATE TABLE ontime  
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

# `dbListTables()` will tell you what tables (if any) are in your database.
dbListTables(conn=db.connection)
# `dbListFields()` will tell you the column names of a specified table
#   in your database.
dbListFields(conn=db.connection, name="ontime")

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
airline.csv88 <- read.csv(file=airline.bzfile)
# Using `system.time()`:
#    user  system elapsed 
# 173.803   4.355 215.931 

# Remove downloaded file from working directory
file.remove(file="airline88.csv")

# Add csv file to your table in your database.
dbWriteTable(conn=db.connection, name='ontime', 
             value=airline.csv88, append=TRUE, row.names=FALSE)

Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql` 
  dbGetQuery(conn=db.connection, statement=sql.command)
}

# Indexes are useful for faster searching.
# See `https://www.sqlite.org/queryplanner.html` for more info
Sql("CREATE INDEX YearIndex 
    ON ontime(Year);")

Sql("CREATE INDEX DateIndex 
    ON ontime(Year, Month, DayOfMonth);")

Sql("CREATE INDEX OriginIndex 
    ON ontime(Origin);")

Sql("CREATE INDEX DestIndex 
    ON ontime(Dest);")

# Tells us how many rows in data.
# `rowid` is automatically generated when creating database.
Sql("SELECT rowid FROM ontime 
    ORDER BY rowid 
    DESC LIMIT 1;")

# Check results
# First 10
Sql("SELECT * FROM ontime 
    LIMIT 10;")

# Last 10
Sql("SELECT * FROM ontime 
    ORDER BY rowid
    DESC LIMIT 10;")

# Close connection.
dbDisconnect(conn=db.connection)
