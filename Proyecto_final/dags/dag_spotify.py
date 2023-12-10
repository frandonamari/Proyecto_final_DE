from datetime import datetime, timedelta
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.entregafinalprueba import get_token, create_df , load_to_sql
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

    # create_tables_task = PostgresOperator(
    #     task_id="create_table",
    #     postgres_conn_id="coderhouse_redshift_1",
    #     sql="sql/create_table.sql",
    #     hook_params={	
    #         "options": "-c search_path=fran_d_donamari_coderhouse"
    #     }
    # )



    get_token_task = PythonOperator(
        task_id="get_token",
        python_callable= get_token,
        op_kwargs={
            "url": "https://accounts.spotify.com/api/token"
        }
    )

    # create_json_task = PythonOperator(
    #     task_id="create_json",
    #     python_callable=create_json,
    #     op_kwargs={
    #         "url_play": "https://accounts.spotify.com/api/token",

    #     }
    # )

    create_df_task = PythonOperator(
        task_id="create_df",
        python_callable=create_df,
        op_kwargs={
            "url_play": "https://accounts.spotify.com/api/token",

        }
    )
    load_to_sql_task = PythonOperator(
        task_id="load_to_sql",
        python_callable=load_to_sql,
        op_kwargs={
            "df_spotify": "create_df(json_result)",
            "table_name": "top_50_global_stg",
            "engine": "conn",
            "table_name": "append"

        }
    )

    dummy_end_task = DummyOperator(
        task_id="dummy_end"
        )


    dummy_start_task >> get_token_task
    get_token_task >> create_df_task
    create_df_task >> load_to_sql_task
    load_to_sql_task >> dummy_end_task
    
