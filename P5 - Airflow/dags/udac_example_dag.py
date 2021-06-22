from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (StageToRedshiftOperator) 
# , LoadFactOperator,LoadDimensionOperator, DataQualityOperator,StageToRedshiftOperator,S3ToRedshiftOperator
from helpers.sql_queries import SqlQueries
from operators.stage_redshift import StageToRedshiftOperator
#from operators.s3_to_redshift import S3ToRedshiftOperator  -- for testing
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_airflow_etl_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_string="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A",
    json_string="auto"
)

#start_operator >> copy_trips_task


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.user_table_insert,
    append_data = False,
    table_name= "public.users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.song_table_insert,
    append_data = False,
    table_name= "public.songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.artist_table_insert,
    append_data = False,
    table_name= "public.artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.time_table_insert,
    append_data = False,
    table_name= "public.time"
)



run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tablelist=["public.songplays","public.users","public.songs","public.artists","public.time"],
    dq_checks=[
        {'sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0,"description":"userid_not_null"},
        {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM users", 'expected_result': 1,"description":"large_than_zero"},
        {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM songplays", 'expected_result': 1,"description":"large_than_zero"},
        {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM songs", 'expected_result': 1,"description":"large_than_zero"},
        {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM artists", 'expected_result': 1,"description":"large_than_zero"},
        {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM time", 'expected_result': 1,"description":"large_than_zero"} 
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >>  load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator