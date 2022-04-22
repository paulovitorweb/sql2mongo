from typing import Union, Tuple


class Incorporator:
    _class = None

    def __init__(
        self,
        source_table: str,
        fk_col: str,
        mapping: Union[dict, str],
        field_name: str,
        scalar: bool = False,
    ):
        self.source_table = source_table
        self.fk_col = fk_col
        self.mapping = mapping
        self.field_name = field_name

        if scalar and not isinstance(self.mapping, str):
            raise ValueError(
                'If migration is scalar you should just provide a string as mapping value'
            )

        self._scalar = scalar

    @property
    def query(self) -> str:
        raise NotImplementedError

    def __str__(self):
        return self.field_name


class OneToManyIncorporator(Incorporator):
    _class = list

    @property
    def query(self) -> str:
        if self._scalar:
            cols = ', '.join([self.mapping, self.fk_col])
        else:
            cols = ', '.join(list(self.mapping.keys()) + [self.fk_col])
        return f'SELECT {cols} FROM {self.source_table}'

    def _format_incorporated_document(self, row: Tuple) -> dict:
        return dict(
            zip(
                tuple(self.mapping.values()),
                row,
            )
        )


class OneToOneIncorporator(Incorporator):
    _class = dict
