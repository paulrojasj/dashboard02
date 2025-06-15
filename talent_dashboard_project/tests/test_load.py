import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from unittest.mock import MagicMock
import pandas as pd
from etl.load import load_df


class DummyConn:
    def __init__(self):
        self.connection = object()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        pass

class DummyEngine:
    def begin(self):
        return DummyConn()


def test_load_df_calls_to_sql(monkeypatch):
    df = pd.DataFrame({'a': [1]})
    engine_mock = DummyEngine()
    to_sql_called = {}

    def fake_get_engine():
        return engine_mock

    def fake_to_sql(*args, **kwargs):
        to_sql_called['called'] = True

    monkeypatch.setattr('etl.load.get_engine', fake_get_engine)
    monkeypatch.setattr('sqlalchemy.inspect', lambda engine: MagicMock(has_table=lambda name: False))
    monkeypatch.setattr(pd.DataFrame, 'to_sql', fake_to_sql)

    load_df(df, 'test_table')

    assert to_sql_called.get('called', False)
