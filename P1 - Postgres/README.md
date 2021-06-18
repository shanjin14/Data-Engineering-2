### Introduction
We will be implementing a postgres RDMS for a dimensional model for a Sparkify database

### How to run
1. Run "python create_table.py"
  1. Note: The script will drop and create a database each time it runs, you can turn this off by running "python create_table.py False"
3. Follow by "python etl.py"

### File in repository
1. etl.py -- the ETL Process
2. sql_queries.py -- consist all the drop, create, insert queries
3. create_table.py -- the create db and table process
