from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import logging
import shutil

from etl.extract import get_excel_files, read_excel
from etl.transform import transform
from etl.load import load_df

RAW_DATA_DIR = Path(__file__).resolve().parents[1] / 'data' / 'raw_data'
PROCESSED_DIR = Path(__file__).resolve().parents[1] / 'data' / 'processed'
LOG_DIR = Path(__file__).resolve().parents[1] / 'logs'

for d in [RAW_DATA_DIR, PROCESSED_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def cargar_datos(**context):
    files = get_excel_files(RAW_DATA_DIR)
    table_map = {
        'empleados': 'empleados',
        'movimientos': 'movimientos_personal',
        'formaciones': 'formaciones',
        'evaluaciones': 'evaluaciones_desempeno',
    }
    for file in files:
        try:
            df = read_excel(file)
            df = transform(df)
            key = file.stem.split('_')[0]
            table_name = table_map.get(key)
            if table_name:
                load_df(df, table_name)
            else:
                raise ValueError(f'No table mapping for {file.name}')
            shutil.move(str(file), PROCESSED_DIR / file.name)
        except Exception as exc:
            logging.exception('Error processing %s', file)
            shutil.move(str(file), LOG_DIR / file.name)
            raise exc


default_args = {
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    'load_talent_data',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
) as dag:

    cargar_task = PythonOperator(
        task_id='cargar_excel_a_postgres',
        python_callable=cargar_datos
    )
