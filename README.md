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

Ou ainda:

```javascript
person = {
  name: 'John Doe',
  adresses: [
    {
      description: '80 Delancey St, New York, NY',
      type: 'WORK',
    },
    {
      description: '2311 Morris Ave, 777, Birmingham, AL',
      type: 'HOME',
    },
  ],
}
```

O objetivo é fornecer um recurso que abstraia essa lógica, permitindo a migração de dados com ou sem incorporação, para aproveitamento do formato de documento, evitando fazer uma cópia indiscriminada do modelo de dados relacional dentro do modelo não relacional, sem refletir sobre o possível ganho de uma remodelagem. Por exemplo:

```python
from sql2mongo import Migration, OneToManyIncorporator

mapping = {
    'Name': 'name',
    'Email': 'email',
    'Adresses': OneToManyIncorporator(
        source_table='Adresses',
        fk_col='UserId',
        field_name='adresses',
        mapping={'Description': 'description', 'Type': 'type'}
    )
}
migration = Migration(mapping, source_table='Users', to_collection='users', pk_col='Id')
assert migration.query == 'SELECT Id, Name, Email FROM Users'
```

Considerando as configurações de migração acima e os seguintes dados presentes num banco relacional:

### Tabela Users

| Id  | Name     | Email         |
| --- | -------- | ------------- |
| 1   | John Doe | john@mail.com |

### Tabela Adresses

| Id  | Description                          | Type | UserId |
| --- | ------------------------------------ | ---- | ------ |
| 1   | 80 Delancey St, New York, NY         | WORK | 1      |
| 2   | 2311 Morris Ave, 777, Birmingham, AL | HOME | 1      |

A migração produzirá um documento no seguinte formato:

```javascript
{
  _id: ObjectId(),
  _old_id: 1,
  name: 'John Doe',
  email: 'john@mail.com',
  adresses: [
    {
      description: '80 Delancey St, New York, NY',
      type: 'WORK',
    },
    {
      description: '2311 Morris Ave, 777, Birmingham, AL',
      type: 'HOME',
    },
  ],
}
```
