from pathlib import Path
import pandas as pd

RAW_DATA_DIR = Path(__file__).resolve().parents[1] / 'data' / 'raw_data'


def get_excel_files(directory: Path = RAW_DATA_DIR):
    return list(directory.glob('*.xlsx'))


def read_excel(path: Path) -> pd.DataFrame:
    return pd.read_excel(path)
