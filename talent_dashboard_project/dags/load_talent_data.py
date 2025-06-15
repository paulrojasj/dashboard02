import sys
from pathlib import Path

# Ensure project root is on the Python path so ETL modules can be imported
# When the DAG is executed from within the Airflow ``dags`` folder, the project
# root is two levels above this file. Adding it to ``sys.path`` allows imports
# such as ``from etl import ...`` to work correctly regardless of where the DAG
# file is located.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import shutil

from etl.extract import get_excel_files, read_excel
from etl.transform import transform
from etl.load import load_df

RAW_DATA_DIR = PROJECT_ROOT / 'data' / 'raw_data'
PROCESSED_DIR = PROJECT_ROOT / 'data' / 'processed'
LOG_DIR = PROJECT_ROOT / 'logs'

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
