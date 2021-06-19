# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays  
(songplay_id SERIAL NOT NULL , 
start_time timestamp NOT NULL, 
user_id integer NOT NULL, 
level VARCHAR , 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id integer, 
location VARCHAR, 
user_agent VARCHAR,
PRIMARY KEY (songplay_id)
)
""")

user_table_create = ("""
create table if not exists users 
(user_id integer NOT NULL, 
first_name VARCHAR, 
last_name VARCHAR , 
gender VARCHAR, 
level VARCHAR,
PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
create table if not exists songs 
(song_id varchar NOT NULL , 
title  VARCHAR NOT NULL, 
artist_id VARCHAR NOT NULL , 
year integer , 
duration double precision,
PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE if not exists artists 
(artist_id VARCHAR NOT NULL  , 
name varchar, 
location varchar , 
lattitude  Decimal(8,6), 
longitude Decimal(9,6),
PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE time
(start_time timestamp NOT NULL, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer , 
weekday integer,
PRIMARY KEY (start_time)
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id ,level ,song_id ,artist_id,session_id ,location,user_agent) VALUES (%s, %s, %s,%s, %s, %s,%s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level ) VALUES (%s, %s, %s,%s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title , artist_id, year , duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name ,location,lattitude,longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday) VALUES (%s, %s, %s,%s, %s, %s,%s)
""")

# FIND SONGS

song_select = ("""
SELECT song_id,A.artist_id FROM songs A join artists B on A.artist_id=B.artist_id 
where title=%s and name=%s and duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]