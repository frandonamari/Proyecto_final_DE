import json
import base64
import requests as r
import pandas as pd
import sqlalchemy as sa
import logging
from configparser import ConfigParser
from sqlalchemy import inspect
from sqlalchemy import create_engine


def get_auth_header(token): #Header para acceder a la informacion de la api
  return{"Authorization": "Bearer " + token }


def build_conn_string(config_path, config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Lee el archivo de configuración
    parser = ConfigParser()
   
   # me informa si existe algun error
    try:
        parser.read(config_path)
        config = parser[config_section]
    except Exception as e:
        print(f"Error al leer el archivo de configuración: {e}")
        return None

    # Lee la sección de configuración de PostgreSQL

    print(f"Host: {config['host']}")
    print(f"Port: {config['port']}")
    print(f"DBName: {config['dbname']}")
    print(f"Username: {config['username']}")
    print(f"Password: {config['pwd']}")
    
    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    # Construye la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'
    
    return conn_string
    



def connect_to_db(config_dir, config_section):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parameters:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.

    Returns:
    sqlalchemy.engine.base.Engine: Un objeto de conexión a la base de datos.
    """
   
   
    logging.info("Conectándose a la base de datos...")
    engine = sa.create_engine("postgresql://fran_d_donamari_coderhouse:3U3iOidaC0@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database")
    conn = engine.connect()
    logging.info("Conexión a la base de datos establecida exitosamente")
    return conn, engine




def load_to_sql(df, table_name, engine, if_exists ="replace"):
    """
    Cargar un dataframe en una tabla de base de datos,
    usando una tabla intermedia o stage para control de duplicados.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    check_field (str): El nombre de la columna que se usará para controlar duplicados.
    """
    try:
        with engine.connect() as conn, conn.begin():
            logging.info(f"Cargando datos en la tabla {table_name}...")
            conn.execute(f"TRUNCATE TABLE {table_name}")
            df.to_sql(
                f"{table_name}", conn,
                if_exists=if_exists, method="multi",
                index=False
                )
            logging.info(f"Datos cargados exitosamente")
            logging.info(f"Cargando datos en la tabla {table_name}...")
            conn.execute(f"""
                MERGE INTO top_50_global_dim
                USING top_50_global_stg
                ON top_50_global_dim.track_id = top_50_global_stg.track_id
                WHEN MATCHED THEN
                    UPDATE SET
                        track_id = top_50_global_stg.track_id,
                        track_name= top_50_global_stg.track_name,
                        artist_name = top_50_global_stg.artist_name,
                        album_name = top_50_global_stg.album_name,
                        album_release_date = top_50_global_stg.album_release_date,
                        album_popularity = top_50_global_stg.album_popularity,
                        updated_at = CURRENT_TIMESTAMP,
                        created_at = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (track_id, track_name, artist_name, album_name, album_release_date, album_popularity, updated_at,created_at)
                    VALUES (top_50_global_stg.track_id, top_50_global_stg.track_name, top_50_global_stg.artist_name, top_50_global_stg.album_name, top_50_global_stg.album_release_date, top_50_global_stg.album_popularity, GETDATE(), GETDATE())
                """)
            logging.info("Datos cargados exitosamente")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")
        logging.error(f"Error al cargar los datos en la base de datos: {e}")