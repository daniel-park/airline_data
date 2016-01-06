

# The following shows how to create SQLite database by directly downloading
#   bz2 files.

# Make sure that you have following packages downloaded.
#   i.e. `install.packages("RSQLite")`
library(RSQLite)
library(readr) # for a quicker read of csv files using `read_csv()`

###############################################################################
# ATTENTION!!!
# This is the part you need to change.
# Set working directory
setwd("/Users/DanielPark/Documents/R_Projects/DataSciAirline")
###############################################################################

# `airline_file` is the designated file name given to the downloaded file.
# The downloaded file will be saved in your working directory.
airline.file <- "airline88.csv"

# `airline_url` is the specific web url where the zipped file is located.
airline.url <- "http://stat-computing.org/dataexpo/2009/1988.csv.bz2"

# Establishing a connection that will allow you to communicate 
#   with the database.
# Because there is no existing database, an empty database named 
#   `airline88.sqlite` will be created.
db.connection <- dbConnect(SQLite(), dbname="airline88.sqlite")
# At the end of this R script, the connection will be closed.
# I have no idea why that's necessary.
# But now that an empty database has been created, running the same exact 
#   `dbConnect()` command will not create a new database named 
#   `airline88.sqlite`.  It will merely re-establish that connection.

# Creating the basic structure for our table which will be in our database.
# Note that multiple tables can exist in a database.
# Table name is `airline88_tbl`
dbSendQuery(conn = db.connection,
            "CREATE TABLE airline88_tbl  
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
            Diverted varchar(1),
            CarrierDelay INTEGER,
            WeatherDelay INTEGER,
            NASDelay INTEGER,
            SecurityDelay INTEGER,
            LateAircraftDelay INTEGER
            )") 

# `dbListTables()` will tell you what tables (if any) are in your database.
dbListTables(db.connection)
# `dbListFields()` will tell you the column names of a specified table
#   in your database.
dbListFields(db.connection, "airline88_tbl")


# Download file
# Recall that the variables `airline_url` and `airline_file` were 
#   created above.
# `download.file` will generate the csv file named `airline88.csv`,
#    which will appear in your working directory.
download.file(url=airline.url, destfile=airline.file)

# Create csv file which will be loaded into memory.
airline.csv88 <- read_csv(bzfile(airline.file))

# W/o using `setOldClass()`, an error occurs.
# Not clear on what `setOldClass()` does.
# See 
#   https://github.com/rstats-db/RSQLite/issues/82
#   for explanation.  
setOldClass(c("tbl_df", "data.frame"))

# Add csv file to your table in your database.
dbWriteTable(conn=db.connection, name='airline88_tbl', 
             value=airline.csv88, append=TRUE, row.names = FALSE)

Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql` 
  dbGetQuery(conn=db.connection, sql.command)
}

# Indexes are useful for faster searching.
Sql("CREATE INDEX YearIndex 
    ON airline88_tbl(Year);")

Sql("CREATE INDEX DateIndex 
    ON airline88_tbl(Year, Month, DayOfMonth);")

Sql("CREATE INDEX OriginIndex 
    ON airline88_tbl(Origin);")

Sql("CREATE INDEX DestIndex 
    ON airline88_tbl(Dest);")

# Tells us how many rows in data.
# `rowid` is automatically generated when creating database.
Sql("SELECT rowid FROM airline88_tbl 
    ORDER BY rowid 
    DESC LIMIT 1;")

# Check results
# First 10
Sql("SELECT * FROM airline88_tbl 
    LIMIT 10;")

# Last 10
Sql("SELECT * FROM airline88_tbl 
    ORDER BY rowid
    DESC LIMIT 10;")


# Close connection.
dbDisconnect(db.connection)
