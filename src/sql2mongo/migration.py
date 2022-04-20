import importlib
from typing import List, Iterator
from src.sql2mongo.incorporator import Incorporator


Rows = Iterator[dict]


class Migration:
    def __init__(
        self,
        mapping: dict,
        source_table: str,
        to_collection: str,
        pk_col: str = 'id',
        pk_field: str = '_old_id',
    ):
        self.mapping = mapping
        self.source_table = source_table
        self.to_collection = to_collection
        self.pk_col = pk_col
        self.pk_field = pk_field
        self.cols: List[str] = self._get_cols()
        self.fields: List[str] = self._get_fields()

        # Mocked
        self._source_session = importlib.import_module('tests.conftest').TestingSessionLocal()
        self._database = importlib.import_module('tests.conftest').test_client['to.db']

        self._data: Rows = []

    def exec_query(self) -> None:
        rows = self._source_session.execute(self.query)
        self._transform_data(rows)

    def _transform_data(self, rows) -> None:
        self._data = (dict(zip(self.fields, row)) for row in rows)

    def insert_data(self) -> None:
        self._database[self.to_collection].insert_many(self._data)

    @property
    def query(self) -> str:
        cols = ', '.join(self.cols)
        return f'SELECT {cols} FROM {self.source_table}'

    def _get_cols(self) -> List[str]:
        return [self.pk_col] + [
            k for (k, v) in self.mapping.items() if not isinstance(v, Incorporator)
        ]

    def _get_fields(self) -> List[str]:
        return [self.pk_field] + [str(f) for f in self.mapping.values()]

    @property
    def data(self) -> Iterator:
        return self._data
