# Part 1:
# The following steps show how to create an SQLite database and add data to it 
#   from unzipped csv files in your working directory.
# This data will include airline data from 1987 - 2008. 
library(RSQLite)
library(dplyr) # for using dplyr functions on SQLite database
library(data.table) # for a fast read of csv files using `fread()`

setwd("/Users/DanielPark/Documents/R_Data/airline/airline_csv")

# The names of the unzipped csv files in your working directory
airline.files <- c("1987.csv", "1988.csv", "1989.csv", "1990.csv", "1991.csv",
                   "1992.csv", "1993.csv", "1994.csv", "1995.csv", "1996.csv",
                   "1997.csv", "1998.csv", "1999.csv", "2000.csv", "2001.csv",
                   "2002.csv", "2003.csv", "2004.csv", "2005.csv", "2006.csv",
                   "2007.csv", "2008.csv")


# Create database
db.connection <- dbConnect(SQLite(), dbname="airline_old.sqlite")


# Following table template from 
# http://stat-computing.org/dataexpo/2009/sqlite.html
dbSendQuery(conn = db.connection,
            "CREATE TABLE airline_table
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
            LateAircraftDelay INTEGER)") 

dbListTables(db.connection) # The tables in the database
dbListFields(db.connection, "airline_table") # Columns in `airline_table`

db_append <- function(file.names) {
  # This function iteratively appends each csv file to the airline database.
  for (one.file in file.names){ # for each year in airline.files
    data.by.year <- fread(input=one.file, header = TRUE) # read in data
    # Append annual airline data to SQLite database:
    dbWriteTable(conn = db.connection, name = 'airline_table', 
                 value = data.by.year, append=TRUE, row.names = FALSE)
  }
}

# Creating database took a little over 20 minutes
db_append(airline.files)
#    user   system  elapsed 
# 644.087   61.512 1339.590 

Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql` 
  dbGetQuery(conn=db.connection, sql.command)
}

# Indexes are useful for faster searching
# Each one takes a lot of time - between 20 and 30 min
Sql("CREATE INDEX YearIndex 
    ON airline_table(Year);") # Did not time this operation

Sql("CREATE INDEX DateIndex 
    ON airline_table(Year, Month, DayOfMonth);")
#    user   system  elapsed 
# 469.381   77.647 1642.357 

Sql("CREATE INDEX OriginIndex 
    ON airline_table(Origin);")
#    user   system  elapsed 
# 355.468   74.276 1303.538 

Sql("CREATE INDEX DestIndex 
    ON airline_table(Dest);")
#     user   system  elapsed 
#  349.429   72.590 1298.135 

# Tells us how many rows in data
# `rowid` is automatically generated when creating database
Sql("SELECT rowid FROM airline_table 
    ORDER BY rowid 
    DESC LIMIT 1;")

# If we want to use dplyr on SQLite database:
dplyr.connection <- src_sqlite("airline_db.sqlite")
airline.dplyr.table <- tbl(dplyr.connection, "airline_table")
# Pulling data for American Airlines:
american.airlines <- filter(airline.dplyr.table, UniqueCarrier=="AA")
# If we want to issue SQL commands:
tbl(dplyr.connection, sql("SELECT * FROM airline_table LIMIT 100"))

# Close connection
dbDisconnect(db.connection)


###############################################################################
# Part 2:
# The following steps show how to create an SQLite database and add data to it
#   by directly downloading bz2 files.
# This data will include airline data from 1987 - 2008. 
# NOTE: I have never tested this part out.
library(RSQLite)
library(readr) # for a quicker read of csv files using `read_csv()`

setwd("/Users/DanielPark/Documents/R_Data/airline/airline_csv")

# The names of the unzipped csv files in your working directory
airline.files <- c("1987.csv", "1988.csv", "1989.csv", "1990.csv", "1991.csv",
                   "1992.csv", "1993.csv", "1994.csv", "1995.csv", "1996.csv",
                   "1997.csv", "1998.csv", "1999.csv", "2000.csv", "2001.csv",
                   "2002.csv", "2003.csv", "2004.csv", "2005.csv", "2006.csv",
                   "2007.csv", "2008.csv")

airline.urls <- c("http://stat-computing.org/dataexpo/2009/1987.csv.bz2", 
                  "http://stat-computing.org/dataexpo/2009/1988.csv.bz2", 
                  "http://stat-computing.org/dataexpo/2009/1989.csv.bz2", 
                  "http://stat-computing.org/dataexpo/2009/1990.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1991.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1992.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1993.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1994.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1995.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1996.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1997.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1998.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/1999.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2000.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2001.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2002.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2003.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2004.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2005.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2006.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2007.csv.bz2",
                  "http://stat-computing.org/dataexpo/2009/2008.csv.bz2")

# Create database
db.connection <- dbConnect(SQLite(), dbname="airline_old.sqlite")


# Following table template from 
# http://stat-computing.org/dataexpo/2009/sqlite.html
dbSendQuery(conn = db.connection,
            "CREATE TABLE airline_table
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
            LateAircraftDelay INTEGER)") 

dbListTables(db.connection) # The tables in the database
dbListFields(db.connection, "airline_table") # Columns in `airline_table`

db_append <- function(file.names) {
  
  for (one.file in file.names){ # for each year in airline.files
    data.by.year <- fread(input=one.file, header = TRUE) # read in data
    # Append annual airline data to SQLite database:
    dbWriteTable(conn = db.connection, name = 'airline_table', 
                 value = data.by.year, append=TRUE, row.names = FALSE)
  }
}

### For direct download
db_direct <- function(file.names, airline.urls) {
  # This function iteratively appends each csv file to the airline database.
  file.count <- length(file.names)
  for (year in 1:file.count){
    download.file(url=airline.urls[year], destfile=file.names[year])
    # `read_csv()` is much faster than `read.csv()`:
    data.by.year <- read_csv(bzfile(file.names[year])) 
    # `setOldClass()` is necessary when using `read_csv()`
    # See
    # https://github.com/rstats-db/RSQLite/issues/82
    #   for more explanation
    setOldClass(c("tbl_df", "data.frame"))
    file.remove(file=file.names[year]) # remove file from working directory
    dbWriteTable(conn = db.connection, name = 'airline_table', 
                 value = data.by.year, append=TRUE, row.names = FALSE)
  }
}


# Never tried, but will probably take hours
db_direct(airline.files, airline.urls)

Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql` 
  dbGetQuery(conn=db.connection, sql.command)
}

# Indexes are useful for faster searching
# Each one takes a lot of time - between 20 and 30 min
Sql("CREATE INDEX YearIndex 
    ON airline_table(Year);")

Sql("CREATE INDEX DateIndex 
    ON airline_table(Year, Month, DayOfMonth);")

Sql("CREATE INDEX OriginIndex 
    ON airline_table(Origin);")

Sql("CREATE INDEX DestIndex 
    ON airline_table(Dest);")


# Tells us how many rows in data
# `rowid` is automatically generated when creating database
Sql("SELECT rowid FROM airline_table 
    ORDER BY rowid 
    DESC LIMIT 1;")

# Close connection
dbDisconnect(db.connection)
