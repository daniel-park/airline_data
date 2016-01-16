# This is a lesson on using SQL commands in R.
# It assumes that an SQLite database has already been set up.

###############################################################################
# Part 1: Basics of setting up SQL environment in R
# Set working directory
setwd("~/Documents/East_Bay/DataScienceClub")
setwd("/Users/DanielPark/Documents/R_Projects/DataSciAirline")

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

###############################################################################
# Part 2: An example of using data from an SQL query
# Collect the total mentions of airline codes for every flight
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

###############################################################################
# Part 3:  Using the `sqldf` package
# If you want to practice using SQL commands on a data frame
library(sqldf)

# Import data from SQLite database
air.data <- Sql("SELECT * FROM ontime")

# Note: if field name (column name) contains a period, it must be contained in
#   quotes.
sqldf("SELECT * FROM 'air.data' 
      LIMIT 10;")

# Close connection.
dbDisconnect(db.connection)
