import pytest
from types import GeneratorType
from src.sql2mongo import Migration


def is_inserted_document(document: dict, expected_document: dict) -> bool:
    _id = document.pop('_id')
    if not _id:
        return False
    return document == expected_document


def test_migration_should_return_formated_data(session):
    session.execute(
        'INSERT INTO Users (Id, Name, Email) '
        'VALUES (1, "John Doe", "jd@mail.com"), (2, "Rod Knee", "rk@mail.com")'
    )
    mapping = {'Name': 'name', 'Email': 'email'}
    mig = Migration(mapping, 'Users', 'users', pk_col='Id')
    mig.exec_query()
    assert isinstance(mig.data, GeneratorType)
    assert next(mig.data) == {'_old_id': 1, 'name': 'John Doe', 'email': 'jd@mail.com'}
    assert next(mig.data) == {'_old_id': 2, 'name': 'Rod Knee', 'email': 'rk@mail.com'}
    with pytest.raises(StopIteration):
        next(mig.data)


def test_migration_should_insert_formated_data(client):
    client, db = client
    mapping = {'Name': 'name', 'Email': 'email'}
    mig = Migration(mapping, 'Users', 'users', pk_col='Id')
    mig.exec_query()
    mig.insert_data()
    documents = db['users'].find()
    assert is_inserted_document(
        next(documents), {'_old_id': 1, 'name': 'John Doe', 'email': 'jd@mail.com'}
    )
    assert is_inserted_document(
        next(documents), {'_old_id': 2, 'name': 'Rod Knee', 'email': 'rk@mail.com'}
    )
