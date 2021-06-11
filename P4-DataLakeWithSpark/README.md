### Project Overview

The project has two public source file provided by the program:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

The goal is to etl the two raw data into a dimensional model as below:

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
THere are two files:
1. etl.py - it has the ETL script written in Spark
2. dl.cfg- it has the access key and secret for interacting with AWS cluster

