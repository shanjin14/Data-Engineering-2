import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_raw_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_raw_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fdn_fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS fdn_dim_users"
song_table_drop = "DROP TABLE IF EXISTS fdn_dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS fdn_dim_artists"
time_table_drop = "DROP TABLE IF EXISTS fdn_dim_times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stg_raw_events 
(
eventId bigint IDENTITY(0,1),
artist varchar,
auth  varchar,
firstName  varchar,
gender  varchar,
iteminSession varchar,
lastName  varchar,
length varchar,
level  varchar,
location  varchar,
method  varchar,
page  varchar,
registration varchar,
sessionid integer,
song  varchar,
status integer,
ts bigint,
userAgent  varchar,
userId integer
) DISTSTYLE AUTO;
""")

#{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", #"song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = ("""
CREATE TABLE stg_raw_songs 
(num_songs integer,
artist_id VARCHAR,
artist_latitude Decimal(8,6),
artist_longitude Decimal(9,6),
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration double precision,
year integer
) DISTSTYLE AUTO;
""")

songplay_table_create = ("""
CREATE TABLE fdn_fact_songplays 
(songplay_id bigint IDENTITY(0,1) NOT NULL distkey, 
start_time timestamp NOT NULL, 
user_id integer NOT NULL, 
level nvarchar(20) , 
song_id varchar, 
artist_id VARCHAR, 
session_id integer, 
location nvarchar(100), 
user_agent nvarchar(max),
PRIMARY KEY (songplay_id)
) SORTKEY AUTO;
""")

user_table_create = ("""
CREATE TABLE fdn_dim_users 
(user_id integer NOT NULL, 
first_name nvarchar(100), 
last_name nvarchar(100) distkey, 
gender nvarchar(10), 
level nvarchar(20),
PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE fdn_dim_songs 
(song_id varchar NOT NULL , 
title  VARCHAR NOT NULL, 
artist_id VARCHAR NOT NULL distkey, 
year integer sortkey, 
duration double precision,
PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE fdn_dim_artists 
(artist_id VARCHAR NOT NULL distkey , 
name varchar, 
location varchar sortkey, 
lattitude  Decimal(8,6), 
longitude Decimal(9,6),
PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE fdn_dim_times
(start_time timestamp NOT NULL distkey, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer , 
weekday integer,
PRIMARY KEY (start_time)

) SORTKEY AUTO;
""")

# STAGING TABLES
ARNSTRING = config['IAM_ROLE']['ARN']
staging_events_copy = ("""
    copy stg_raw_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON region 'us-west-2';
""").format(config['S3']['LOG_DATA'],ARNSTRING,config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy stg_raw_songs from {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON region 'us-west-2';
""").format(config['S3']['SONG_DATA'],ARNSTRING,"'auto'")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fdn_fact_songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
        ,userId,level,song_id,artist_id, sessionId, location, userAgent
                            from stg_raw_events A 
                            LEFT JOIN stg_raw_songs songs
                              ON A.song = songs.title
                              AND A.artist = songs.artist_name
                              AND A.length = songs.duration
                            where page='NextSong'
                            ;
""")

#As the user can upgrade and downgrade th account the level can switch from free to paid to free and incur multiple row
#use CTAS to get latest unique row
#follow the idea or truncate and load
#alternative practice should be follow SCD type 3
user_table_insert = ("""
INSERT INTO fdn_dim_users
WITH UniqueUsers (userId, firstName, lastName, gender, level,RID)
AS (
select distinct userId, firstName, lastName, gender, level,row_number() over (partition by userId order by ts desc) as RID from stg_raw_events
where page='NextSong' 
)

Select userId, firstName, lastName, gender, level from UniqueUsers
where RID =1
;
""")

song_table_insert = ("""
INSERT INTO fdn_dim_songs
select distinct song_id, title, artist_id, year, duration  from stg_raw_songs;
""")

artist_table_insert = ("""
INSERT INTO fdn_dim_artists
select distinct artist_id, artist_name, artist_location, artist_latitude , artist_longitude  from stg_raw_songs;
""")

time_table_insert = ("""
INSERT INTO fdn_dim_times
select distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
extract(hour from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
extract(day from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
extract(week from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
extract(month from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
extract(year from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'),
extract(dow from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')
from stg_raw_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
