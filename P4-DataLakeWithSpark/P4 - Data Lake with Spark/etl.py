import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType,DateType
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/"
    
    # read song data file
    df = spark.read.json(song_data+'A/A/*/*.json')

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table = spark.sql("select song_id, title, artist_id, year, duration from song_data")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").option("header", "true").mode("overwrite").parquet(output_data+"fdn_dim_songs")

    # extract columns to create artists table
    artists_table = spark.sql("select artist_id, artist_name, artist_location, artist_latitude \
                            , artist_longitude  from song_data")
    
    # write artists table to parquet files
    artists_table.write.option("header", "true").mode("overwrite").parquet(output_data+"fdn_dim_artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data+"log_data/"

    # read log data file
    df = spark.read.json(log_data+'*/*/*.json')
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data")
    df=spark.sql("Select * from log_data where page='NextSong'") 
    df.createOrReplaceTempView("log_data_NextSong")
    
    # extract columns for users table    
    users_table = spark.sql("select userId, firstName, lastName, gender, level from log_data_NextSong")
    
    # write users table to parquet files
    users_table.write.option("header", "true").mode("overwrite").parquet(output_data+"fdn_dim_users")

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("timestamp",(F.col("ts") / 1000).cast(TimestampType()))
      
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn("datetime",F.col("timestamp").cast(DateType())) \
               .withColumn("start_time",F.date_format('timestamp', 'HH:mm:ss')) \
               .withColumn("hour",F.hour("timestamp")) \
                .withColumn("day",F.dayofmonth("timestamp")) \
                .withColumn("week",F.weekofyear("timestamp")) \
                .withColumn("month",F.month("timestamp")) \
                .withColumn("year",F.year("timestamp")) \
                .withColumn("weekday",F.dayofweek("timestamp")) 
    
    df.createOrReplaceTempView("log_data_NextSong_timestamp")
    #start_time, hour, day, week, month, year, weekday
    # extract columns to create time table
    time_table = spark.sql("select distinct start_time,hour,day,week,month, year,weekday from log_data_NextSong_timestamp")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.option("header", "true").mode("overwrite").parquet(output_data+"fdn_dim_time")
    
    
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # read in song data to use for songplays table
    song_df = spark.read.option("basePath", output_data+"fdn_dim_songs").parquet(output_data+"fdn_dim_songs/*/*/*.parquet")
    song_df.createOrReplaceTempView("fdn_dim_songs")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("select row_number() over (order by start_time,sessionId,userId) as songplay_id, \
                            start_time,userId,artist_id, sessionId, location, userAgent,A.year,A.month \
                            from log_data_NextSong_timestamp A \
                            join fdn_dim_songs B on A.song=B.title")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").option("header", "true").mode("overwrite").parquet(output_data+"fdn_fact_songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-foundation/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
