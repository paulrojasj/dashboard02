import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Basic transformations: parse columnas de fecha."""
    for col in df.columns:
        if 'fecha' in col:
            df[col] = pd.to_datetime(df[col]).dt.date
    return df
