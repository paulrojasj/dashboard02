import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).resolve().parents[1] / '.env')

DB_URL = os.getenv('DATABASE_URL')

def get_engine():
    return create_engine(DB_URL)


def load_df(df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    logging.info('Loaded %s records into %s', len(df), table_name)
