from datetime import datetime, timedelta
import sqlalchemy as sa
from airflow.models import DAG, Variable
from email import message
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.entregafinalprueba import create_df , df_spotify
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib


def enviar_informacion_por_correo(**kwargs):
    try:
       # Obtener el DataFrame del contexto
        ti = kwargs['ti']
        df_spotify = ti.xcom_pull(task_ids='create_df', key='return_value')

        # Mensajes de depuración
        print("DataFrame recibido:")
        print(df_spotify.head())  # Imprimir las primeras filas del DataFrame

        # Verificar que 'artist_name' está presente en el DataFrame
        if 'artist_name' not in df_spotify.columns:
            raise ValueError("La columna 'artist_name' no está presente en el DataFrame.")

        resumen = df_spotify.groupby('artist_name').size().reset_index(name='cantidad')

         # Mensajes de depuración
        print("Resumen:")
        print(resumen.head())

       # Crear el mensaje del correo electrónico
        mensaje = MIMEMultipart()
        mensaje['From'] = "fran.d.donamari@gmail.com"
        mensaje['To'] = "fran.d.donamari@gmail.com"
        mensaje['Subject'] = 'Resumen de Cantidad por Artista'

      # Agregar el cuerpo del mensaje con el resumen de la cantidad por artista
        cuerpo_mensaje = MIMEText(resumen.to_html(index=False), 'html', 'utf-8')  # Asegúrate de que no incluya el índice
        cuerpo_mensaje['Content-Transfer-Encoding'] = 'quoted-printable'
        mensaje.attach(cuerpo_mensaje)

        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('fran.d.donamari@gmail.com',
                 Variable.get('GMAIL_SECRET')
        
         )    

        x.sendmail("fran.d.donamari@gmail.com", "fran.d.donamari@gmail.com", mensaje.as_string())

        # Cerrar la conexión
        x.quit()

        print('Exito')

    except Exception as exception:
        print(exception)
        print('Failure')


default_args = {
    "owner": "Frandonamari",
    "description": "Api que toma el top 50 de canciones globales en Spotify",
    "depend_on_past": False,
    "email": ['fran.d.donamari@gmail.com'],
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_spotify_catchup",
    start_date=datetime(2023, 12, 7),
    catchup=True,
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    
    create_df_task = PythonOperator(
        task_id="create_df",
        python_callable=create_df,
        dag=dag,
        provide_context=True
    )
 
    send_emailresume_task = PythonOperator(
        task_id="send_emailresume_task",
        python_callable=enviar_informacion_por_correo,
        dag=dag,
        provide_context=True
    )

create_df_task >> send_emailresume_task