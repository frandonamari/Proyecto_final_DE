import json
import base64
import requests as r
import pandas as pd
import sqlalchemy as sa
import logging
from configparser import ConfigParser
from utils import *
from sqlalchemy.engine import Engine

url_play = "https://api.spotify.com/v1/playlists/"
config_dir = "Proyecto_final\\config\\config.ini"
congif_section = "redshift"

def get_token(): # Permite generar un token de autenticación
    clientid = '8fee91e6afc8464a8542acb7742fbab5'
    clientsecret = '31196df9cad546d4ad6d7b6e455f8547'
    auth_string = clientid + ":" + clientsecret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes),"utf-8")


    url = "https://accounts.spotify.com/api/token"

    headers= {

        "Authorization": "Basic " + auth_base64,
        "Content-type": "application/x-www-form-urlencoded"

    }

    data= {

        "grant_type": "client_credentials"

    }

    result = r.post(url,headers=headers,data=data)
    json_result = result.json()
    print(json_result)
    token =json_result['access_token']
    return token



def create_df():
    
    token = get_token()
    url_play = "https://api.spotify.com/v1/playlists/"
    headers = get_auth_header(token)
    playlist ='37i9dQZEVXbMDoHDwVN2tF'
    web= url_play + playlist
    result = r.get(web,headers=headers)
    json_result = result.json()

    try:  
        stations_data = json_result["tracks"]["items"]
        data = {"track_id":[],"track_name":[],"artist_name":[],"album_name":[],"album_release_date":[],"album_popularity":[]}
        for i in (stations_data):
            data["track_id"].append(i["track"]["id"][:5])
            data["track_name"].append(i["track"]["name"])
            data["artist_name"].append(i["track"]["artists"][0]["name"])
            data["album_name"].append(i["track"]["album"]["name"])
            data["album_release_date"].append(i["track"]["album"]["release_date"])
            data["album_popularity"].append(i["track"]["popularity"])

        #Armado del dataframe
        df_spotify = pd.DataFrame(data)
        return df_spotify               
    except Exception as e:
        logging.error(f"Error al obtener datos de {url_play}: {e}")
        raise e
    
df_spotify = create_df()
config_dir = "Proyecto_final\\config\\config.ini"
conn_string = build_conn_string(config_dir, "redshift") 
conn, engine = connect_to_db(config_dir, "redshift")        # Definir engine con un valor predeterminado
load_to_sql(df_spotify, "top_50_global_stg", conn, "append")          

# def load_data(config_dir):
#     df_spotify = create_df()
#     logging.info("Conectándose a la base de datos...")
#     conn, engine = build_conn_string (config_dir, "redshift")
#     logging.info("Conexión a la base de datos establecida exitosamente")
#     load_to_sql(df_spotify, "top_50_global_stg", conn, "append")
#     conn.close()


if __name__ == "__entregafinalprueba__":
    
    df_spotify = create_df()
    