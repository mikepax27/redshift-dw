## Udacity - Data Engineering Nanodegree Program
# Project: Data Warehouse (using Redshift)

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I was tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Project Description

In this project, I applied what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I needed to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. Tw S3 sources, song_data and log_data, consist in json files used to populated the 5 tables listed below.

**Fact Table**

1. songplays - records in event data associated with song plays i.e. records with page NextSong, extracted from log data.
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

2. users - users in the app, extracted from log data.
    - user_id, first_name, last_name, gender, level
3. songs - songs in music database, extracted from song data.
    - song_id, title, artist_id, year, duration
4. artists - artists in music database, extracted from song data.
    - artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units, extracted from log data.
    - start_time, hour, day, week, month, year, weekday

In the process, two staging tables are used to accommodate data read from json files called staging_events, staging_songs. These tables are then used to populate the five tables listed above.

## Project Files

- **dwh.cfg** - Configuration file with the information about the Redshift cluster, the IAM role and the S3 used.
- **sql_queries.py** - File with the queries to drop and create the tables, in addition to the ones responsible for loading the data in them.
- **create_table.py** - This script drops all the tables (if exist) and re-create new ones.
- **etl.py** - Script responsible for load data from S3 into staging tables on Redshift and then process that data into the data warehouse's tables on Redshift.

## How to run
1. Insert the missing data about your aws account at the **`dwh.cfg`** file.
2. Run the **`create_tables.py`** file to create the tables ate the redshift.
    
       python create_tables.py

3. Execute ETL process by running **`etl.py`**.

       python etl.py
