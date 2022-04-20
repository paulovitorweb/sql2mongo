from src.sql2mongo import Migration, OneToManyIncorporator


def test_migration_instance():
    mapping = {'src_name': 'name', 'src_email': 'email'}
    mig = Migration(mapping, 'Users', 'users')
    assert mig.mapping == mapping
    assert mig.source_table == 'Users'
    assert mig.to_collection == 'users'
    assert mig.cols == ['id', 'src_name', 'src_email']
    assert mig.fields == ['_old_id', 'name', 'email']
    assert mig.query == 'SELECT id, src_name, src_email FROM Users'
    assert mig.data == []


def test_migration_instance_without_default_values():
    mapping = {'Name': 'name', 'Email': 'email'}
    mig = Migration(mapping, 'Users', 'users', pk_col='Id', pk_field='old_id')
    assert mig.cols == ['Id', 'Name', 'Email']
    assert mig.fields == ['old_id', 'name', 'email']
    assert mig.query == 'SELECT Id, Name, Email FROM Users'
    assert mig.data == []


def test_incorporator_instance():
    inc = OneToManyIncorporator(
        source_table='Adresses',
        fk_col='UserId',
        field_name='adresses',
        mapping={'publicArea': 'street'},
    )
    assert inc.query == 'SELECT publicArea, UserId FROM Adresses'


def test_migration_instance_with_incorporator():
    mapping = {
        'Name': 'name',
        'Email': 'email',
        'Adresses': OneToManyIncorporator(
            source_table='Adresses',
            fk_col='UserId',
            field_name='adresses',
            mapping={'publicArea': 'street'},
        ),
    }
    mig = Migration(mapping, 'Users', 'users', pk_col='Id')
    assert mig.cols == ['Id', 'Name', 'Email']
    assert mig.fields == ['_old_id', 'name', 'email', 'adresses']
    assert mig.query == 'SELECT Id, Name, Email FROM Users'