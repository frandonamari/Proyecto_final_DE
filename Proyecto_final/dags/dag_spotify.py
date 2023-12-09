from datetime import datetime, timedelta
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.entregafinalprueba import get_token, create_json, connect_to_db , create_df 

default_args = {
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

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="dummy_start"
        )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="coderhouse_redshift_1",
        sql="sql/create_table.sql",
        hook_params={	
            "options": "-c search_path=fran_d_donamari_coderhouse"
        }
    )



    get_token_task = PythonOperator(
        task_id="get_token",
        python_callable= get_token,
        op_kwargs={
            "url": "https://accounts.spotify.com/api/token"
        }
    )

    create_json_task = PythonOperator(
        task_id="create_json",
        python_callable=create_json,
        op_kwargs={
            "url_play": "https://accounts.spotify.com/api/token",

        }
    )

    dummy_end_task = DummyOperator(
        task_id="dummy_end"
        )


    dummy_start_task >> create_tables_task
    create_tables_task >> get_token_task
    get_token_task >> create_json_task
    create_json_task >> dummy_end_task
    