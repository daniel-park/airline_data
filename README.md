# airline_data
The R scripts in this repository analyze US airline on-time performance.  The data come from
http://stat-computing.org/dataexpo/2009/
and
http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236

**airline88sqlite.R**  
Loads the 1988 data from scratch.

**RSQLite_Tutorial.R**  
Gives a brief overview of how to access a local database using R and the RSQLite package.  It assumes that the 1988 airline data already exists in a database.

**airline_db87-08.R**  
Shows how to use the RSQLite package to create SQLite database for airline data 1987-2008.  Two methods are shown:  

1) Uses the unzipped csv files in your local directory.  
2) Directly downloads data from http://stat-computing.org/dataexpo/2009/

**airline_db2011-present.R**  
Shows how to use the RSQLite package to create SQLite database for airline data 2011-present (Nov 2015).  Data comes from http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236 .  There are more variables in this data than the data used in airline_db87-08.R.  This script assumes files have already been downloaded and unzipped.
