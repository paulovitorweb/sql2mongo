import importlib
from typing import List, Iterator, Tuple
from pymongo.collection import Collection
from src.sql2mongo.incorporator import Incorporator, OneToManyIncorporator


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

        self._incorporators: List[Incorporator] = self._get_incorporators()

        # Mocked
        self._source_session = importlib.import_module('tests.conftest').TestingSessionLocal()
        self._database = importlib.import_module('tests.conftest').test_client['to.db']

        self._data: Rows = []

    def exec(self) -> None:
        self._exec_query()
        self._insert_data()
        if self._incorporators:
            self._exec_incorporations()

    @property
    def query(self) -> str:
        cols = ', '.join(self.cols)
        return f'SELECT {cols} FROM {self.source_table}'

    @property
    def data(self) -> Iterator:
        return self._data

    def _exec_query(self) -> None:
        rows = self._source_session.execute(self.query)
        self._transform_data(rows)

    def _exec_incorporations(self) -> None:
        for inc in self._incorporators:
            if not isinstance(inc, OneToManyIncorporator):
                raise NotImplementedError('Only one-to-many incorporations are supported')

            collection = self._get_collection()
            rows = self._source_session.execute(inc.query)
            if inc._scalar:
                for row in rows:
                    filt = {self.pk_field: row[-1]}
                    collection.update_one(filt, {'$push': {inc.field_name: row[0]}})
            else:
                for row in rows:
                    filt = {self.pk_field: row[-1]}
                    document = inc._format_incorporated_document(row)
                    collection.update_one(filt, {'$push': {inc.field_name: document}})

    def _transform_data(self, rows) -> None:
        if self._incorporators:
            self._data = (dict(zip(self.fields, self.__format_document(row))) for row in rows)
        else:
            self._data = (dict(zip(self.fields, row)) for row in rows)

    def _insert_data(self) -> None:
        self._get_collection().insert_many(self._data)

    def _get_cols(self) -> List[str]:
        return [self.pk_col] + [
            k for (k, v) in self.mapping.items() if not isinstance(v, Incorporator)
        ]

    def _get_fields(self) -> List[str]:
        return [self.pk_field] + [str(f) for f in self.mapping.values()]

    def _get_incorporators(self) -> List[Incorporator]:
        return [v for v in self.mapping.values() if isinstance(v, Incorporator)]

    def _get_collection(self) -> Collection:
        return self._database[self.to_collection]

    def __format_document(self, row) -> Tuple:
        return tuple(row) + tuple(inc._class() for inc in self._incorporators)
