from pathlib import Path
import pandas as pd
from sqlalchemy import text
from tableauhyperapi import HyperProcess, Connection, TableDefinition, TableName, SqlType, Inserter, Telemetry

from .load import get_engine

TABLES = [
    'empleados',
    'movimientos_personal',
    'formaciones',
    'evaluaciones_desempeno'
]

OUTPUT_DIR = Path(__file__).resolve().parents[1] / 'data' / 'tableau'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def df_to_hyper(table_name: str, df: pd.DataFrame, connection: Connection):
    cols = [TableDefinition.Column(col, SqlType.text()) for col in df.columns]
    table_def = TableDefinition(TableName(table_name), cols)
    connection.catalog.create_table(table_def)
    with Inserter(connection, table_def) as inserter:
        inserter.add_rows(df.itertuples(index=False, name=None))
        inserter.execute()


def export_tables_to_hyper(hyper_path: Path, tables=None):
    tables = tables or TABLES
    engine = get_engine()
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=True) as connection:
            for table in tables:
                df = pd.read_sql(text(f'SELECT * FROM {table}'), engine)
                df_to_hyper(table, df, connection)
    return hyper_path


if __name__ == '__main__':
    hyper_file = OUTPUT_DIR / 'talent_data.hyper'
    export_tables_to_hyper(hyper_file)
    print(f'Hyper file generated at {hyper_file}')
