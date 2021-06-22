# Data-Engineering


### Introduction
This repository is part of the Data Engineer Nanodegree. Most of the work is done on AWS.

### Pre-requisites
1. Spin up the redshift cluster in AWS
2. run the create_table.sql using AWS query editor to pre-create all the destination tables
3. spin up the airflow instance

### How to Run
1. Turn on the DAG in Airflow UI
2. It will run automatically since the schedule is hourly

### interesting notes
1. We coded custom operator for all the ETL atomic steps. As the Airflow sometime will cache the old code, your revised operator may not get loaded and you will get error "Broken DAG: [xxx.py] cannot import name YourRevisedOperator"
  1. To avoid this issue, I directly import the operator (such as "from operators.load_fact import LoadFactOperator")
  2. Reference: https://www.astronomer.io/guides/airflow-importing-custom-hooks-operators
