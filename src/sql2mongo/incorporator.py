class Incorporator:
    def __init__(self, source_table: str, fk_col: str, mapping: dict, field_name: str):
        self.source_table = source_table
        self.fk_col = fk_col
        self.mapping = mapping
        self.field_name = field_name

    @property
    def query(self) -> str:
        raise NotImplementedError

    def __str__(self):
        return self.field_name


class OneToManyIncorporator(Incorporator):
    @property
    def query(self) -> str:
        cols = ', '.join(list(self.mapping.keys()) + [self.fk_col])
        return f'SELECT {cols} FROM {self.source_table}'
