### Project Overview

The project has two public source file provided by the program:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

The goal is to etl the two raw data into a dimensional model as below so that the data can be analysed such as what song is most played ..etc

##### Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
##### Dimension Tables
- users - users in the app
  - user_id, first_name, last_name, gender, level
- songs - songs in music database
  - song_id, title, artist_id, year, duration
- artists - artists in music database
  - artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

### File in repository
There are 5 files:
1. create_tables.py -- create the table
2. dwh.cfg - store configuration such as Redshift address..etc
3. etl.py -- etl job to execute the query
4. sql_queries.py - consist of all the sql query to be executed
2. DevDW.ipynb - a exploratory workbook to explore 

### ETL flow
The etl job flow as below:
1. Create all the table with specified schema
2. read the data from the source and use COPY to into redshift
3. transform the data to the dimensional model above and insert into the table

### Prerequisites
1. Redshift cluster is created to run
2. Python environment is installed (such as Anaconda)

### Deployment (How to run?)
1. Key in the information needed in the dwh.cfg
2. Run "python create_tables.py"
3. Run "python etl.py"

### Additional
1. You probably would like to explore the data in S3 a little bit before create the staging table for log_data and song_data. 
  To do so, you can use boto3 SDK in python to read the file in S3 and see a sample of it.
  Code example can be found in DevDW.ipynb

### Key Considerations
1. Data type - 
2. Distribution Key & Sort Key- Distribution key determine how the data are distributed across node. Sort Key determine how the data is sorted.
   To determine both, you would probably need to profile the data and understand how the data been access in order to determine the better distribution key & sort key.
   Redshift allows you to alter the SORTKEY, DISTKEY after creation [link to Redshift Documentation on this](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html)
   Side note: Allow SORTKEY, DISTKEY in alter table is great for Redshift. If we are using Azure Synapse, we would need to CTAS to recreate a new table ,  rename the table, drop the table [Link here](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-distribute#re-create-the-table-with-a-new-distribution-column)

### Interesting part
1. As the user can upgrade and downgrade th account the level can switch from free to paid to free and incur multiple row in the log events.
2. In order to deduplicate and get the latest status, we will use CTAS to get the most latest row
3. Having said that, I think it would be better SCD type 4 construction. We can create a "stg_raw_users_history" which have all the historical status
4. After that we create a dimension view call "fdn_dim_users" which query the "stg_raw_users_history" to get the latest information
5. If the table do become a bottleneck for query speed, we can create materialized view and refresh it each time the data load is run.

### Future improvement 
1. This is implemented using Truncate and Load. If we want to move to incremental load, I think we would need additional to capture what are changed such as CDC at source system.
