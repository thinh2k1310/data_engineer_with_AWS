from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from udacity.common.final_project_sql_statements import SqlQueries
import psycopg2

start_date = datetime(2018, 11, 1)
end_date = datetime(2018, 11, 30)

s3_bucket = "thinhtcq"
events_s3_key = "log-data/2018/11/"
songs_s3_key = "song-data/A/A/"

default_args = {
    'owner': 'thinhtcq',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

def create_redshift_tables():
    # Connect to the Redshift database
    redshift = PostgresHook(postgres_conn_id="redshift")
    connection = redshift.get_connection()
    conn = psycopg2.connect(
        host=connection.host,
        port=connection.port,
        database=connection.schema,
        user=connection.login,
        password=connection.password
    )

    # Open a cursor to execute SQL commands
    cur = conn.cursor()

    # Read the SQL script file
    with open('/home/workspace/airflow/dags/udacity/common/create_tables.sql', 'r') as file:
        sql_script = file.read()

    # Execute the SQL script
    cur.execute(sql_script)

    # Commit the changes
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_redshift_tables_task = PythonOperator(
        task_id='create_redshift_tables',
        python_callable=create_redshift_tables
    )
   

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=events_s3_key
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id = "redshift",
        tables = ["songplays", "users", "songs", "artists", "time"]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> create_redshift_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> end_operator

final_project_dag = final_project()