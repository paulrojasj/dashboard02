import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from unittest.mock import patch, MagicMock
import pandas as pd
from etl.load import load_df


def test_load_df_calls_to_sql(monkeypatch):
    df = pd.DataFrame({'a': [1]})
    engine_mock = MagicMock()
    to_sql_called = {}

    def fake_create_engine(url):
        return engine_mock

    def fake_to_sql(*args, **kwargs):
        to_sql_called['called'] = True

    monkeypatch.setattr('etl.load.create_engine', fake_create_engine)
    monkeypatch.setattr(pd.DataFrame, 'to_sql', fake_to_sql)

    load_df(df, 'test_table')

    assert to_sql_called.get('called', False)
