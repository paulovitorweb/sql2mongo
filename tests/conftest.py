import pytest
import mongomock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URL = 'sqlite:///./test.db'

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={'check_same_thread': False})
TestingSessionLocal = sessionmaker(autocommit=True, autoflush=False, bind=engine)

test_client = mongomock.MongoClient()


@pytest.fixture(scope='session')
def session():
    session = TestingSessionLocal()
    session.execute(
        'CREATE TABLE Users '
        '(Id int NOT NULL, Name varchar(255) NOT NULL, Email varchar(255) NOT NULL)'
    )
    session.execute(
        'CREATE TABLE Adresses '
        '(Id int NOT NULL, Description varchar(255), City varchar(64), UserId int NOT NULL)'
    )
    yield session
    session.execute('DROP TABLE Users')
    session.execute('DROP TABLE Adresses')


@pytest.fixture(scope='session')
def client():
    db = test_client['to.db']
    db.create_collection('users')
    yield test_client, db
    test_client.close()
