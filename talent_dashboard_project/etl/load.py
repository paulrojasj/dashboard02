import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).resolve().parents[1] / '.env')

DB_URL = os.getenv('DATABASE_URL')

def get_engine():
    print(f"DB_URL: {DB_URL}")
    engine = create_engine(DB_URL)
    print(f"Engine: {engine}")
    return engine


def load_df(df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
    engine = get_engine()
    from sqlalchemy.types import String, Date, Integer, Numeric
    from sqlalchemy import inspect
    dtype_map = None
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name)
    # Forzar tipos de columnas a str donde corresponda
    if table_name == 'empleados':
        df['id_empleado'] = df['id_empleado'].astype(str)
        df['nombre'] = df['nombre'].astype(str)
        df['apellido'] = df['apellido'].astype(str)
        df['area'] = df['area'].astype(str)
        df['cargo'] = df['cargo'].astype(str)
        df['tipo_contrato'] = df['tipo_contrato'].astype(str)
        dtype_map = {
            'id_empleado': String(),
            'nombre': String(),
            'apellido': String(),
            'area': String(),
            'cargo': String(),
            'fecha_ingreso': Date(),
            'tipo_contrato': String()
        }
    elif table_name == 'movimientos_personal':
        df['id_movimiento'] = df['id_movimiento'].astype(str)
        df['id_empleado'] = df['id_empleado'].astype(str)
        df['tipo_movimiento'] = df['tipo_movimiento'].astype(str)
        df['motivo'] = df['motivo'].astype(str)
        dtype_map = {
            'id_movimiento': String(),
            'id_empleado': String(),
            'tipo_movimiento': String(),
            'fecha': Date(),
            'motivo': String()
        }
    elif table_name == 'formaciones':
        df['id_formacion'] = df['id_formacion'].astype(str)
        df['id_empleado'] = df['id_empleado'].astype(str)
        df['curso'] = df['curso'].astype(str)
        dtype_map = {
            'id_formacion': String(),
            'id_empleado': String(),
            'curso': String(),
            'horas': Integer(),
            'fecha': Date()
        }
    elif table_name == 'evaluaciones_desempeno':
        df['id_evaluacion'] = df['id_evaluacion'].astype(str)
        df['id_empleado'] = df['id_empleado'].astype(str)
        df['evaluador'] = df['evaluador'].astype(str)
        dtype_map = {
            'id_evaluacion': String(),
            'id_empleado': String(),
            'puntaje': Numeric(3,2),
            'fecha': Date(),
            'evaluador': String()
        }
    if table_exists:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, method='multi')
    else:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, dtype=dtype_map, method='multi')
    logging.info('Loaded %s records into %s', len(df), table_name)
