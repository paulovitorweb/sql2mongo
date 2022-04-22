import pytest
from src.sql2mongo import Migration, OneToManyIncorporator


def is_inserted_document(document: dict, expected_document: dict) -> bool:
    _id = document.pop('_id')
    if not _id:
        return False
    return document == expected_document


def up(session):
    session.execute(
        'INSERT INTO Users (Id, Name, Email) '
        'VALUES (1, "John Doe", "jd@mail.com"), (2, "Rod Knee", "rk@mail.com")'
    )
    session.execute(
        'INSERT INTO Adresses (Id, Description, City, UserId) '
        'VALUES (1, "Adress A", "A city", 1), (2, "Adress B", "Other city", 1), (3, "Adress C", "A city", 2)'
    )


def down(session, client):
    session.execute('DELETE FROM Users')
    session.execute('DELETE FROM Adresses')
    client[1]['users'].delete_many({})


class TestMigration:
    @pytest.fixture(scope='class')
    def mig(self, session, client):
        up(session)
        mapping = {'Name': 'name', 'Email': 'email'}
        sut = Migration(mapping, 'Users', 'users', pk_col='Id')
        sut.exec()
        yield sut
        down(session, client)

    def test_migration_data(self, mig, client):
        _, db = client
        documents = db['users'].find()
        assert is_inserted_document(
            next(documents), {'_old_id': 1, 'name': 'John Doe', 'email': 'jd@mail.com'}
        )
        assert is_inserted_document(
            next(documents), {'_old_id': 2, 'name': 'Rod Knee', 'email': 'rk@mail.com'}
        )


class TestMigrationWithScalarOneToManyIncorporation:
    @pytest.fixture(scope='class')
    def mig(self, session, client):
        up(session)
        mapping = {
            'Name': 'name',
            'Email': 'email',
            'Adresses': OneToManyIncorporator(
                'Adresses',
                fk_col='UserId',
                mapping='Description',
                scalar=True,
                field_name='adresses',
            ),
        }
        sut = Migration(mapping, 'Users', 'users', pk_col='Id')
        sut.exec()
        yield sut
        down(session, client)

    def test_migration_data_with_scalar_one2many_incorporation(self, mig, client):
        _, db = client
        documents = db['users'].find()
        assert is_inserted_document(
            next(documents),
            {
                '_old_id': 1,
                'name': 'John Doe',
                'email': 'jd@mail.com',
                'adresses': ['Adress A', 'Adress B'],
            },
        )
        assert is_inserted_document(
            next(documents),
            {'_old_id': 2, 'name': 'Rod Knee', 'email': 'rk@mail.com', 'adresses': ['Adress C']},
        )


class TestMigrationWithOneToManyIncorporation:
    @pytest.fixture(scope='class')
    def mig(self, session, client):
        up(session)
        mapping = {
            'Name': 'name',
            'Email': 'email',
            'Adresses': OneToManyIncorporator(
                'Adresses',
                fk_col='UserId',
                mapping={'Description': 'description', 'City': 'city'},
                field_name='adresses',
            ),
        }
        sut = Migration(mapping, 'Users', 'users', pk_col='Id')
        sut.exec()
        yield sut
        down(session, client)

    def test_migration_data_with_one2many_incorporation(self, mig, client):
        _, db = client
        documents = db['users'].find()
        assert is_inserted_document(
            next(documents),
            {
                '_old_id': 1,
                'name': 'John Doe',
                'email': 'jd@mail.com',
                'adresses': [
                    {'description': 'Adress A', 'city': 'A city'},
                    {'description': 'Adress B', 'city': 'Other city'},
                ],
            },
        )
        assert is_inserted_document(
            next(documents),
            {
                '_old_id': 2,
                'name': 'Rod Knee',
                'email': 'rk@mail.com',
                'adresses': [{'description': 'Adress C', 'city': 'A city'}],
            },
        )
