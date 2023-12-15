from datetime import datetime, timedelta
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.entregafinalprueba import create_df 
default_args = {
    "owner":"Frandonamari",
    "description":"Api que toma el top 50 de canciones globales en Spotify",
    "depend_on_past":False,
    "email": ['fran.d.donamari@gmail.com'],
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
    }

with DAG(
    dag_id="dag_spotify_catchup",
    start_date=datetime(2023, 12, 7),
    catchup=True,
    schedule_interval= "@daily",
    default_args=default_args
) as dag:

    create_df_task = PythonOperator(
        task_id="create_df",
        python_callable=create_df,
        op_kwargs={
            "url_play": "https://accounts.spotify.com/api/token",

        }
    )

  

    