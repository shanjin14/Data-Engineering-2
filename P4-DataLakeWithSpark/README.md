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
1. etl.py - it has the ETL script written in PySpark
2. dl.cfg- it has the access key and secret for interacting with AWS cluster

### ETL flow
The etl job flow as below:
1. read the data from the source
2. perform the transformation into the dimensional above
3. write the data back into s3 bucket

### Prerequisites
1. Two s3 bucket is create:
- sparkify-foundation - this is the bucket to store the foundation table that are in parquet format
- sparkkfy-code - this is the place to put the code and the dl.cfg


### Deployment (How to run?)
1. Launch the EMR cluster comes with Notebook (it will be handy for testing part of your code if you face any issue)
- Note: remember to choose a EC2 key pair. It is required to SSH putty into the Hadoop cluster to do the work
2. SSH into the master using putty. Instruction is [here] (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html)
3. below is the aws command to run in the terminal after you SSH into your masternode:
  1. aws s3 cp s3://sparkify-code ./ --recursive #it copy all the file inthe sparkify-code bucket
  2. python-submit etl.py
4. Fix any issue based on the error you see...


### Future Improvement
The current ETL job is one-time load script. We would need to add in checks to perform either options below:
1. Incremental Load - songplays , artists
2. Truncate and Load - songs, times, users since they are small table
3. Use Airflow to orchestrate the spark job

