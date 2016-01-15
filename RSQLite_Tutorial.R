# This is a lesson on using SQL commands in R.
# It assumes that an SQLite database has already been set up.

# Set working directory
setwd("~/Documents/East_Bay/DataScienceClub")

# Load `RSQLite` library to enable SQL-related functions
library(RSQLite)

# Establish connection with your SQLite database.
db.connection <- dbConnect(drv=SQLite(), dbname="ontime.sqlite3")

# If your database was not located in your working directory, 
#   you can specify a file path:
#db.connection <- dbConnect(drv=SQLite(), 
#                           dbname="~/Documents/East_Bay/DataScienceClub/ontime.sqlite3")

# `dbListTables()` will tell you what tables (if any) are in your database.
dbListTables(conn=db.connection)
# `dbListFields()` will tell you the column names of a specified table
#   in your database.
dbListFields(conn=db.connection, name="ontime")


# Basic query
dbGetQuery(conn=db.connection, 
           statement="SELECT ArrDelay FROM ontime LIMIT 10;")

# Average distance of flight
dbGetQuery(conn=db.connection, 
           statement="SELECT avg(Distance) FROM ontime;")

# `dbGetQuery()` returns a data frame
first10rows <- dbGetQuery(conn=db.connection, 
                          statement="SELECT * FROM ontime LIMIT 10;")
str(first10rows) # Examine structure


Sql <- function(sql.command) {
  # Simplifies typing for SQL queries
  # Note dplyr package contains a function `sql` 
  dbGetQuery(conn=db.connection, statement=sql.command)
}

# Simplified query
Sql("SELECT ArrDelay FROM ontime LIMIT 10;")

# Semicolon not necessary
Sql("SELECT ArrDelay FROM ontime LIMIT 10")

# When quotes are necessary in a query
Sql("SELECT COUNT(UniqueCarrier='UA') FROM ontime;")

# Collect the airline code for each flight
airline.counts <- Sql("SELECT UniqueCarrier FROM ontime;")

# Count total airline code
(total.table <- table(airline.counts))

# Download table with airline codes and corresponding airline names
airline.code <- read.csv(file="http://stat-computing.org/dataexpo/2009/carriers.csv", 
                         stringsAsFactors=FALSE)
str(airline.code) # Examine structure

# Narrow airline codes/names down to codes cited in 1988 airline data
airlines.of.interest <- airline.code[airline.code$Code %in% names(total.table),]

# Convert table to a data frame.
# Necessary for merging data together.
table.df <- as.data.frame(total.table)

# Merge count data and code/name data
airline.totals <- merge(x=airlines.of.interest, 
                        y=table.df, by.x="Code",by.y="airline.counts")

airline.totals$Description # Examine names

# Edit selected airline names
airline.totals[airline.totals$Code=="HP","Description"] <- "America West Airlines Inc."
airline.totals[airline.totals$Code=="US","Description"] <- "US Airways Inc."

# Divide totals by 1000 so that numbers are in the 100s of flights instead of
#   100,000s of flights:
airline.totals$Freq <- round(airline.totals$Freq/1000)

# plot the number of flights for each airline
library(ggplot2)
ggplot(data=airline.totals, aes(x=reorder(Description,Freq), y=Freq)) + 
  geom_bar(stat="identity") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title="Number of Flights by Airline for 1988", 
         x="Airline", y="Frequency \n (in thousands of flights)")














library(RCurl) # for `getURL()` function
# required to handle 'https' addresses:
airport.url <- getURL("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat")
latlong <- read.csv(header=FALSE, text=airport.url)
airport.header <- c("id", "name", "city", "country", "code", "ICAOcode", 
                    "latitude", "longitude", "altitude", "timezone", 
                    "daysavtime", "olson_tz")
names(latlong) <- airport.header
head(latlong)

uslatlong <- latlong[latlong$country=="United States",
                     c("name", "city", "code", "latitude", "longitude")]
rownames(uslatlong) <- NULL

library(ggplot2)
library(maps)
# load US map data
all_states <- map_data("state")

cancelled <- Sql("SELECT Origin, Dest
                 FROM ontime
                 WHERE Cancelled=1;")

total.cancel <- table(c(cancelled$Origin, cancelled$Dest))
cancel.total <- as.data.frame(total.cancel)
names(cancel.total) <- c("code", "freq")

cancel.loc <- merge(cancel.total, uslatlong, by="code")
cancel.loc48 <- cancel.loc[cancel.loc$longitude > -125,]

p <- ggplot() + 
  geom_polygon(data=all_states, aes(x=long, y=lat, group=group),colour="white", fill="grey50") +
  geom_point(data=cancel.loc48, aes(x=longitude, y=latitude, size=freq), color="black") + 
  scale_size(name="Cancellations") +
  geom_text(data=cancel.loc48, hjust=0.5, vjust=-0.5, aes(x=longitude, y=latitude, label=ifelse(freq>2000,as.character(city),'')), size=4) +
  theme(axis.line=element_blank(),
        axis.text.x=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks=element_blank(),
        axis.title.x=element_blank(),
        axis.title.y=element_blank()) +
  labs(title="Cancelled Flights in 1988")
p

# Close connection.
dbDisconnect(db.connection)




















x <- sqldf("SELECT * FROM airline88 
           WHERE UniqueCarrier = 'PS';")

x <- sqldf("SELECT * FROM airline88 
           WHERE DayofMonth = 8 AND Month = 1;")
