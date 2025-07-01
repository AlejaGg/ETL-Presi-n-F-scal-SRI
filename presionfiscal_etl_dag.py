# Import necessary modules
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from google.cloud import bigquery
import os
from google.oauth2 import service_account
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration Variables ---
PROJECT_ID = "etl-sistemas-sri"
DATASET_ID = "sri_presion_fiscal"
SOURCE_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.sri_presion_fiscal"
table_id_dim_geografia = f"{PROJECT_ID}.{DATASET_ID}.Dim_Geografia"
table_id_dim_tiempo = f"{PROJECT_ID}.{DATASET_ID}.Dim_Tiempo"
table_id_dim_tipopresion = f"{PROJECT_ID}.{DATASET_ID}.Dim_TipoPresion"
table_id_fact = f"{PROJECT_ID}.{DATASET_ID}.Fact_PresionFiscal"
local_filename = "SRI_Presion_Fiscal.csv"
csv_url = "https://www.sri.gob.ec/o/sri-portlet-biblioteca-alfresco-internet/descargar/7e45627e-1f7e-4e21-ae59-d520634fc63f/SRI_Presion_Fiscal.csv"
credential_filepath = "/home/airflow/gcs/data/credentials/etl-sistemas-sri-cc64af72be76.json"

# --- Python Functions for ETL Tasks ---

def download_csv_from_url(csv_url, local_filename):
    logger.info(f"Attempting to download file from: {csv_url}")
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        with open(local_filename, 'wb') as f:
            f.write(response.content)
        logger.info(f"File '{local_filename}' downloaded successfully.")
    except Exception as e:
        logger.error(f"Download task failed: {e}")
        raise ValueError(f"Download task failed: {e}")

def process_dim_geografia(credential_filepath=credential_filepath):
    logger.info("Starting process_dim_geografia task.")
    try:
        credentials = service_account.Credentials.from_service_account_file(credential_filepath)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        schema_dim_geografia = [
            bigquery.SchemaField("id_geografia", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("pais", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
        ]
        query_geografia = f"""
        SELECT DISTINCT `País`, Region FROM `{SOURCE_TABLE_ID}` WHERE `País` IS NOT NULL AND Region IS NOT NULL
        """
        df_geografia = client.query(query_geografia).to_dataframe()
        df_geografia = df_geografia.sort_values(by=['País', 'Region']).reset_index(drop=True)
        df_geografia['id_geografia'] = df_geografia.index + 1
        df_geografia.rename(columns={'País': 'pais', 'Region': 'region'}, inplace=True)
        df_geografia = df_geografia[['id_geografia', 'pais', 'region']]
        job_config = bigquery.LoadJobConfig(schema=schema_dim_geografia, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_geografia, table_id_dim_geografia, job_config=job_config)
        job.result()
        logger.info(f"Successfully loaded to {table_id_dim_geografia}")
    except Exception as e:
        logger.error(f"Error in process_dim_geografia: {e}")
        raise

def process_dim_tiempo(credential_filepath=credential_filepath):
    logger.info("Starting process_dim_tiempo task.")
    try:
        credentials = service_account.Credentials.from_service_account_file(credential_filepath)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        # <<< CORRECCIÓN CLAVE: Usar 'anio' en el esquema para evitar caracteres especiales.
        schema_dim_tiempo = [
            bigquery.SchemaField("tiempo_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("anio", "INT64", mode="REQUIRED"),
        ]
        query_tiempo = f"SELECT DISTINCT `Año` FROM `{SOURCE_TABLE_ID}` WHERE `Año` IS NOT NULL"
        df_tiempo = client.query(query_tiempo).to_dataframe()
        df_tiempo['tiempo_id'] = df_tiempo['Año'].rank(method='dense').astype('Int64')
        # <<< CORRECCIÓN CLAVE: Renombrar la columna a 'anio'.
        df_tiempo.rename(columns={'Año': 'anio'}, inplace=True)
        # <<< CORRECCIÓN CLAVE: Seleccionar la columna renombrada 'anio'.
        df_tiempo = df_tiempo[['tiempo_id', 'anio']]
        job_config = bigquery.LoadJobConfig(schema=schema_dim_tiempo, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_tiempo, table_id_dim_tiempo, job_config=job_config)
        job.result()
        logger.info(f"Successfully loaded to {table_id_dim_tiempo}")
    except Exception as e:
        logger.error(f"Error in process_dim_tiempo: {e}")
        raise

def process_dim_tipopresion(credential_filepath=credential_filepath):
    logger.info("Starting process_dim_tipopresion task.")
    try:
        credentials = service_account.Credentials.from_service_account_file(credential_filepath)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        schema_dim_tipopresion = [
            bigquery.SchemaField("tipo_presion_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("tipo_presion_nombre", "STRING", mode="REQUIRED"),
        ]
        query_tipopresion = f"SELECT DISTINCT Tipo_Presion FROM `{SOURCE_TABLE_ID}` WHERE Tipo_Presion IS NOT NULL"
        df_tipopresion = client.query(query_tipopresion).to_dataframe()
        df_tipopresion['tipo_presion_id'] = df_tipopresion.reset_index().index + 1
        df_tipopresion.rename(columns={'Tipo_Presion': 'tipo_presion_nombre'}, inplace=True)
        df_tipopresion = df_tipopresion[['tipo_presion_id', 'tipo_presion_nombre']]
        job_config = bigquery.LoadJobConfig(schema=schema_dim_tipopresion, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_tipopresion, table_id_dim_tipopresion, job_config=job_config)
        job.result()
        logger.info(f"Successfully loaded to {table_id_dim_tipopresion}")
    except Exception as e:
        logger.error(f"Error in process_dim_tipopresion: {e}")
        raise

def process_fact_presionfiscal(credential_filepath=credential_filepath):
    logger.info("Starting process_fact_presionfiscal task.")
    try:
        credentials = service_account.Credentials.from_service_account_file(credential_filepath)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        query_source_data = f"""
        SELECT Region, `País`, `Año`, `%_Presion`, Tipo_Presion FROM `{SOURCE_TABLE_ID}`
        WHERE Region IS NOT NULL AND `País` IS NOT NULL AND `Año` IS NOT NULL AND `%_Presion` IS NOT NULL AND Tipo_Presion IS NOT NULL
        """
        df_source_data = client.query(query_source_data).to_dataframe()
        df_source_data['Año'] = pd.to_numeric(df_source_data['Año'], errors='coerce').astype('Int64')
        df_source_data['%_Presion'] = pd.to_numeric(df_source_data['%_Presion'], errors='coerce')

        query_dim_geografia = f"SELECT id_geografia, pais, region FROM `{table_id_dim_geografia}`"
        df_dim_geografia = client.query(query_dim_geografia).to_dataframe()

        # <<< CORRECCIÓN CLAVE: Consultar la columna 'anio' de la tabla Dim_Tiempo.
        query_dim_tiempo = f"SELECT tiempo_id, anio FROM `{table_id_dim_tiempo}`"
        # La línea de abajo es la que falla. La consulta AHORA es correcta.
        df_dim_tiempo = client.query(query_dim_tiempo).to_dataframe()

        query_dim_tipopresion = f"SELECT tipo_presion_id, tipo_presion_nombre FROM `{table_id_dim_tipopresion}`"
        df_dim_tipopresion = client.query(query_dim_tipopresion).to_dataframe()

        df_fact_staging = df_source_data.copy()
        df_fact_staging = pd.merge(df_fact_staging, df_dim_geografia, left_on=['País', 'Region'], right_on=['pais', 'region'], how='left')
        # <<< CORRECCIÓN CLAVE: Hacer el merge con la columna 'anio'.
        df_fact_staging = pd.merge(df_fact_staging, df_dim_tiempo, left_on='Año', right_on='anio', how='left')
        df_fact_staging = pd.merge(df_fact_staging, df_dim_tipopresion, left_on='Tipo_Presion', right_on='tipo_presion_nombre', how='left')

        df_fact = df_fact_staging[['id_geografia', 'tiempo_id', 'tipo_presion_id', '%_Presion']].copy()
        df_fact.rename(columns={'%_Presion': 'presion_fiscal'}, inplace=True)
        for col_id in ['id_geografia', 'tiempo_id', 'tipo_presion_id']:
            df_fact[col_id] = pd.to_numeric(df_fact[col_id], errors='coerce').astype('Int64')

        schema_fact = [
            bigquery.SchemaField("id_geografia", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("tiempo_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("tipo_presion_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("presion_fiscal", "FLOAT64", mode="NULLABLE"),
        ]
        job_config = bigquery.LoadJobConfig(schema=schema_fact, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_fact, table_id_fact, job_config=job_config)
        job.result()
        logger.info(f"Successfully loaded to {table_id_fact}")
    except Exception as e:
        logger.error(f"Error in process_fact_presionfiscal: {e}")
        raise

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'presionfiscal_etl',
    default_args=default_args,
    description='ETL pipeline to load SRI tax pressure data to BigQuery',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'bigquery', 'sri'],
) as dag:
    start = EmptyOperator(task_id='start')
    # Nota: si la tabla fuente 'sri_presion_fiscal' ya existe, la tarea de descarga es redundante.
    download_csv_task = PythonOperator(
        task_id='download_source_csv',
        python_callable=download_csv_from_url,
        op_kwargs={'csv_url': csv_url, 'local_filename': local_filename},
    )
    process_dim_geografia_task = PythonOperator(
        task_id='process_dim_geografia',
        python_callable=process_dim_geografia,
    )
    process_dim_tiempo_task = PythonOperator(
        task_id='process_dim_tiempo',
        python_callable=process_dim_tiempo,
    )
    process_dim_tipopresion_task = PythonOperator(
        task_id='process_dim_tipopresion',
        python_callable=process_dim_tipopresion,
    )
    process_fact_presionfiscal_task = PythonOperator(
        task_id='process_fact_presionfiscal',
        python_callable=process_fact_presionfiscal,
    )
    end = EmptyOperator(task_id='end')

    # Dependencias
    start >> download_csv_task
    download_csv_task >> [process_dim_geografia_task, process_dim_tiempo_task, process_dim_tipopresion_task]
    [process_dim_geografia_task, process_dim_tiempo_task, process_dim_tipopresion_task] >> process_fact_presionfiscal_task
    process_fact_presionfiscal_task >> end