# sql2mongo

Um recurso para migrar registros de um banco de dados relacional para o formato de documento do MongoDB.

## Por quê?

Migração de dados de bancos relacionais para o MongoDB pode envolver a necessidade de uma mudança na modelagem dos dados, como incorporar registros dentro de um campo do documento. Por exemplo, considerando uma relação de um para muitos entre uma tabela Person e uma tabela Adress, onde cada registro de pessoa pode estar associado a um ou mais endereços, pode ser interessante incorporar os dados dentro de um único documento, assim:

```javascript
person = {
  name: 'John Doe',
  adresses: [
    '80 Delancey St, New York, NY',
    '2311 Morris Ave, 777, Birmingham, AL',
  ],
}
```

O objetivo é fornecer um recurso que abstraia essa lógica, permitindo a migração de dados com ou sem incorporação, para aproveitamento do formato de documento, evitando fazer uma cópia do modelo de dados relacional dentro do modelo não relacional.

```python
from sql2mongo import Migration, OneToManyIncorporator

mapping = {
    'Name': 'name',
    'Email': 'email',
    'Adresses': OneToManyIncorporator(
        source_table='Adresses',
        fk_col='UserId',
        field_name='adresses',
        mapping={'Street': 'street'}
    )
}

migration = Migration(mapping, source_table='Users', to_collection='users', pk_col='Id')

assert migration.cols == ['Id', 'Name', 'Email']
assert migration.fields == ['_old_id', 'name', 'email', 'adresses']
assert migration.query == 'SELECT Id, Name, Email FROM Users'
```
