

# The following steps show how to add files to a SQLite data base
#   using unzipped csv files in your working directory
library(RSQLite)
library(dplyr) # for using dplyr functions on SQLite database
library(data.table) # for a fast read of csv files using `fread()`

setwd("/Users/DanielPark/Documents/R_Data/airline/airline_csv")

# The names of the unzipped csv files in your working directory
airline.files <- c("On_Time_On_Time_Performance_2011_1.csv",
                   "On_Time_On_Time_Performance_2011_2.csv",
                   "On_Time_On_Time_Performance_2011_3.csv",
                   "On_Time_On_Time_Performance_2011_4.csv",
                   "On_Time_On_Time_Performance_2011_5.csv",
                   "On_Time_On_Time_Performance_2011_6.csv",
                   "On_Time_On_Time_Performance_2011_7.csv",
                   "On_Time_On_Time_Performance_2011_8.csv",
                   "On_Time_On_Time_Performance_2011_9.csv",
                   "On_Time_On_Time_Performance_2011_10.csv",
                   "On_Time_On_Time_Performance_2011_11.csv",
                   "On_Time_On_Time_Performance_2011_12.csv",
                   "On_Time_On_Time_Performance_2012_1.csv",
                   "On_Time_On_Time_Performance_2012_2.csv",
                   "On_Time_On_Time_Performance_2012_3.csv",
                   "On_Time_On_Time_Performance_2012_4.csv",
                   "On_Time_On_Time_Performance_2012_5.csv",
                   "On_Time_On_Time_Performance_2012_6.csv",
                   "On_Time_On_Time_Performance_2012_7.csv",
                   "On_Time_On_Time_Performance_2012_8.csv",
                   "On_Time_On_Time_Performance_2012_9.csv",
                   "On_Time_On_Time_Performance_2012_10.csv",
                   "On_Time_On_Time_Performance_2012_11.csv",
                   "On_Time_On_Time_Performance_2012_12.csv",
                   "On_Time_On_Time_Performance_2013_1.csv",
                   "On_Time_On_Time_Performance_2013_2.csv",
                   "On_Time_On_Time_Performance_2013_3.csv",
                   "On_Time_On_Time_Performance_2013_4.csv",
                   "On_Time_On_Time_Performance_2013_5.csv",
                   "On_Time_On_Time_Performance_2013_6.csv",
                   "On_Time_On_Time_Performance_2013_7.csv",
                   "On_Time_On_Time_Performance_2013_8.csv",
                   "On_Time_On_Time_Performance_2013_9.csv",
                   "On_Time_On_Time_Performance_2013_10.csv",
                   "On_Time_On_Time_Performance_2013_11.csv",
                   "On_Time_On_Time_Performance_2013_12.csv",
                   "On_Time_On_Time_Performance_2014_1.csv",
                   "On_Time_On_Time_Performance_2014_2.csv",
                   "On_Time_On_Time_Performance_2014_3.csv",
                   "On_Time_On_Time_Performance_2014_4.csv",
                   "On_Time_On_Time_Performance_2014_5.csv",
                   "On_Time_On_Time_Performance_2014_6.csv",
                   "On_Time_On_Time_Performance_2014_7.csv",
                   "On_Time_On_Time_Performance_2014_8.csv",
                   "On_Time_On_Time_Performance_2014_9.csv",
                   "On_Time_On_Time_Performance_2014_10.csv",
                   "On_Time_On_Time_Performance_2014_11.csv",
                   "On_Time_On_Time_Performance_2014_12.csv",
                   "On_Time_On_Time_Performance_2015_1.csv",
                   "On_Time_On_Time_Performance_2015_2.csv",
                   "On_Time_On_Time_Performance_2015_3.csv",
                   "On_Time_On_Time_Performance_2015_4.csv",
                   "On_Time_On_Time_Performance_2015_5.csv",
                   "On_Time_On_Time_Performance_2015_6.csv",
                   "On_Time_On_Time_Performance_2015_7.csv",
                   "On_Time_On_Time_Performance_2015_8.csv",
                   "On_Time_On_Time_Performance_2015_9.csv",
                   "On_Time_On_Time_Performance_2015_10.csv",
                   "On_Time_On_Time_Performance_2015_11.csv")




# Create database
db.connection <- dbConnect(SQLite(), dbname="airline_db.sqlite")


# Table is modeled after 
# http://stat-computing.org/dataexpo/2009/sqlite.html
dbSendQuery(conn = db.connection,
            "CREATE TABLE airline_table
            (Year SMALLINT,
            Quarter SMALLINT,
            Month SMALLINT,
            DayofMonth SMALLINT,
            DayOfWeek SMALLINT,
            FlightDate DATE,
            UniqueCarrier VARCHAR(5),
            TailNum VARCHAR(8),
            FlightNum INTEGER,
            Origin CHAR(3),
            OriginCityName VARCHAR(50),
            OriginState CHAR(2),
            OriginStateName VARCHAR(50),
            Dest VARCHAR(3),
            DestCityName VARCHAR(50),
            DestState CHAR(2),
            DestStateName VARCHAR(50),
            CRSDepTime SMALLINT,
            DepTime SMALLINT,
            DepDelay SMALLINT,
            DepDelayMinutes SMALLINT,
            DepDel15 BINARY(1),
            DepartureDelayGroups SMALLINT,
            DepTimeBlk CHAR(9),
            TaxiOut SMALLINT,
            WheelsOff SMALLINT,
            WheelsOn SMALLINT,
            TaxiIn SMALLINT,
            CRSArrTime SMALLINT,
            ArrTime SMALLINT,
            ArrDelay SMALLINT,
            ArrDelayMinutes SMALLINT,
            ArrDel15 BINARY(1),
            ArrivalDelayGroups SMALLINT,
            ArrTimeBlk CHAR(9),
            Cancelled BINARY(1),
            Diverted BINARY(1),
            CRSElapsedTime SMALLINT,
            ActualElapsedTime SMALLINT,
            AirTime SMALLINT,
            Distance INTEGER,
            DistanceGroup SMALLINT)")

dbListTables(db.connection) # The tables in the database
dbListFields(db.connection, "airline_table") # Columns in `airline_table`


db_append <- function(file.names) {
  # This function iteratively appends each csv file to the airline database.
  for (one.file in file.names){ # for each year in airline.files
    data.by.year <- fread(input=one.file, header=TRUE) # read in data
    # Selecting desired columns
    col.reduction <- select(data.by.year, 
                            Year:UniqueCarrier, TailNum, FlightNum, 
                            Origin:OriginState, OriginStateName,
                            Dest:DestState, DestStateName, 
                            CRSDepTime:Cancelled, Diverted:AirTime,
                            Distance, DistanceGroup)
    # Append annual airline data to SQLite database:
    dbWriteTable(conn=db.connection, name='airline_table', 
                 value=col.reduction, append=TRUE, row.names=FALSE)
  }
}

# Creating database took a little over 20 minutes
db_append(airline.files)
#    user   system  elapsed 
# 644.087   61.512 1339.590 

Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql` 
  dbGetQuery(conn=db.connection, statement=sql.command)
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













setwd("~/Documents/R_Projects/DataSciAirline/airline_csv")
airline2009_01 <- fread(input="On_Time_On_Time_Performance_2009_1.csv", header = TRUE)
airline2015_01 <- fread(input="On_Time_On_Time_Performance_2015_1.csv", header = TRUE)
airline2015_11 <- fread(input="On_Time_On_Time_Performance_2015_11.csv", header = TRUE)
airline2011.10 <- fread(input="On_Time_On_Time_Performance_2011_10.csv", header = TRUE)
airline2012.10 <- fread(input="On_Time_On_Time_Performance_2012_10.csv", header = TRUE)
airline2015.11 <- fread(input="On_Time_On_Time_Performance_2015_11.csv", header = TRUE)

names(airline2015.11)
names(airline2012.10)
x <- tolower(gsub("_", "", names(airline2015.11))) == tolower(names(airline2012.10))
names(airline2015.11)[which(x==FALSE, arr.ind = FALSE, useNames = TRUE)]
names(airline2012.10)[which(x==FALSE, arr.ind = FALSE, useNames = TRUE)]

setnames(x=airline2015.11, old=names(airline2015.11), new=names(airline2012.10)) 
names(airline2009_01)
names(airline2015.11)
write.csv(x=airline2015.11, file="On_Time_On_Time_Performance_2015_11.csv",
          row.names=FALSE)


tolower("Div1")
air2011.10 <- select(airline2011.10, 
                     Year:UniqueCarrier, TailNum, FlightNum, 
                     Origin:OriginState, OriginStateName,
                     Dest:DestState, DestStateName, 
                     CRSDepTime:Cancelled, Diverted:AirTime,
                     Distance, DistanceGroup)
air2012.10 <- select(airline2012.10, 
                     Year:UniqueCarrier, TailNum, FlightNum, 
                     Origin:OriginState, OriginStateName,
                     Dest:DestState, DestStateName, 
                     CRSDepTime:Cancelled, Diverted:AirTime,
                     Distance, DistanceGroup)
NY2011 <- filter(air2011.10, (Origin %in% c("JFK", "LGA", "EWR")) & (DayofMonth > 22))
NY2012 <- filter(air2012.10, (Origin %in% c("JFK", "LGA", "EWR")) & (DayofMonth > 29))
sum(NY2011$Cancelled)
sum(NY2012$Cancelled)

airline.code <- read.csv(file="http://stat-computing.org/dataexpo/2009/carriers.csv", 
                         stringsAsFactors=FALSE)

filter(airline.code, Code=="B6") %>% select(Description)
bradley <- filter(air2011.10, FlightNum==504 & UniqueCarrier=="B6")

airline87 <- fread(input="1987.csv", header = TRUE)
subset87 <- airline87[1:100,]

subset2015.11 <- airline2015_11[1:100,]
sum(is.na(airline2015_01$CarrierDelay))
summary(airline2015_01$DistanceGroup)
names(airline2015_01)
table(airline2015_01$Flights)
table(airline2015_01$CancellationCode)
table(airline2015_01$DivAirportLandings)

library(dplyr)
reduced2015 <- select(airline2015_01, 
                      Year:UniqueCarrier, TailNum, FlightNum, 
                      Origin:OriginState, OriginStateName,
                      Dest:DestState, DestStateName, 
                      CRSDepTime:Cancelled, Diverted:AirTime,
                      Distance, DistanceGroup)
reduced100 <- reduced2015[1:100,]
names(reduced2015)

reduced2009 <- select(airline2009_01, 
                      Year:UniqueCarrier, TailNum, FlightNum, 
                      Origin:OriginState, OriginStateName,
                      Dest:DestState, DestStateName, 
                      CRSDepTime:Cancelled, Diverted:AirTime,
                      Distance, DistanceGroup)
str(reduced2015)









dbWriteTable(conn = db.connection, name = 'airline_table', 
             value = reduced2015, append=TRUE, row.names = FALSE)
dbWriteTable(conn = db.connection, name = 'airline_table', 
             value = reduced2009, append=TRUE, row.names = FALSE)

Sql("SELECT rowid FROM airline_2011_15 
    ORDER BY rowid 
    DESC LIMIT 1;")


x <- Sql("SELECT * FROM airline_table 
         LIMIT 100;")
str(x)
unique(x$Diverted)
unique(x$Cancelled)
table(x$Diverted)

x <- filter(reduced2015, TailNum == "N787AA")



summary(as.integer(reduced2015$FlightNum))

unique(reduced2015$Diverted)
header.names <- names(read.csv(file="On_Time_On_Time_Performance_2011_1.csv", nrows=1))
for (i in airline.files){
  current <- names(read.csv(file=i, nrows=1))
  print(i)
  print(current[!(header.names==current)])
  print("Next")
}


names(read.csv(file="On_Time_On_Time_Performance_2009_1.csv", nrows=1))

tr <- c(TRUE, FALSE)
!any(tr)
