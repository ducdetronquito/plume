# Built-in dependencies
import json
import operator
import sqlite3


def _get_nested_value(document: dict, field: str):
    nested_fields = field.split('.')
    value = document
    try:
        for field in nested_fields:
            value = value[field]
        return value
    except KeyError:
        return None


class Selector:

    def get_selector(self, key: str, expression):
        if key[0] != '$':
            if not isinstance(expression, dict):
                return Equal(key, expression)
            elif len(expression) == 1:
                operator, value = expression.popitem()
                return self.get_field_selector(key, operator, value)
            else:
                return ImplicitAnd(key, expression)
        elif key == '$and':
            return And(expression)
        elif key == '$or':
            return Or(expression)

    def get_field_selector(self, field: str, operator: str, value):
        if operator == '$eq':
            return Equal(field, value)
        elif operator == '$gt':
            return GreaterThan(field, value)
        elif operator == '$gte':
            return GreaterThanOrEqual(field, value)
        elif operator == '$lt':
            return LowerThan(field, value)
        elif operator == '$lte':
            return LowerThanOrEqual(field, value)
        elif operator == '$ne':
            return NotEqual(field, value)


class And(Selector):
    __slots__ = ('_selectors',)

    def __init__(self, expressions: list) -> None:
        self._selectors = []
        for expression in expressions:
            key, value = expression.popitem()
            selector = self.get_selector(key, value)
            self._selectors.append(selector)

    def match(self, document: dict) -> bool:
        for selector in self._selectors:
            if not selector.match(document):
                return False
        return True

    def sql(self, indexed_fields: set) -> str:
        index_clauses = []
        non_index_selectors = []
        for selector in self._selectors:
            clause = selector.sql(indexed_fields)
            if clause:
                index_clauses.append(clause)
            else:
                non_index_selectors.append(selector)

        if index_clauses:
            self._selectors = non_index_selectors
            return ' AND '.join(index_clauses)


class ImplicitAnd(And):
    __slots__ = ('_selectors',)

    def __init__(self, field: str, expression: dict) -> None:
        self._selectors = []
        for operator, value in expression.items():
            if operator == '$and':
                selector = And(value)
            elif operator == '$or':
                selector = Or(value)
            else:
                selector = self.get_field_selector(field, operator, value)
            self._selectors.append(selector)


class Or(Selector):
    __slots__ = ('_selectors',)

    def __init__(self, expressions: list) -> None:
        self._selectors = []
        for expression in expressions:
            key, value = expression.popitem()
            selector = self.get_selector(key, value)
            self._selectors.append(selector)

    def match(self, document: dict) -> bool:
        for selector in self._selectors:
            if selector.match(document):
                return True
        return False

    def sql(self, indexed_fields: set) -> str:
        index_clauses = []
        for selector in self._selectors:
            clause = selector.sql(indexed_fields)
            if clause:
                index_clauses.append(clause)

        if len(index_clauses) != len(self._selectors):
            return None

        or_clause = ' OR '.join(clause for clause in index_clauses)
        self._selectors = []
        return or_clause


class ComparisonSelector(Selector):
    __slots__ = ('_field', '_value')

    def __init__(self, field: str, value) -> None:
        self._field = field
        self._value = value

    def match(self, document: dict) -> bool:
        return self._operator(
            _get_nested_value(document, self._field),
            self._value
        )

    def sql(self, indexed_fields: set) -> bool:
        if self._field in indexed_fields:
            return ' '.join([
                '"' + self._field + '"', self._sql_operator, str(self._value)
            ])


class Equal(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '='
    _operator = operator.__eq__

    def sql(self, indexed_fields: set) -> str:
        if self._field not in indexed_fields:
            return None

        if isinstance(self._value, str):
            value = '"' + self._value + '"'
        else:
            value = self._value
        return ' '.join([
            '"' + self._field + '"', self._sql_operator, str(value)
        ])


class GreaterThan(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '>'
    _operator = operator.__gt__


class GreaterThanOrEqual(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '>='
    _operator = operator.__ge__


class LowerThan(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '<'
    _operator = operator.__lt__


class LowerThanOrEqual(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '<='
    _operator = operator.__le__


class NotEqual(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '!='
    _operator = operator.__ne__

    def sql(self, indexed_fields: set) -> str:
        if self._field in indexed_fields:
            if isinstance(self._value, str):
                value = '"' + self._value + '"'
            else:
                value = self._value
            return ' '.join([
                '"' + self._field + '"', self._sql_operator, str(value)
            ])


class SelectQuery:
    __slots__ = ('_indexed_fields', '_limit', '_query_tree', '_table_name')

    def __init__(self, table_name: str, indexed_fields: set,
                 query: dict, limit: int=None) -> None:
        self._table_name = table_name
        self._indexed_fields = indexed_fields
        self._limit = limit
        query = [{key: value} for key, value in query.items()]
        self._query_tree = And(query)

    def match_many(self, documents: list) -> list:
        # If all filters have been applyed on indexed field
        # we can return the documents directly.
        if not self._query_tree._selectors:
            return list(documents)

        return [
            document for document in documents
            if self._query_tree.match(document)
        ]

    def match_one(self, documents: list) -> dict:
        if documents and not self._query_tree._selectors:
            return list(documents)[0]

        for document in documents:
            if self._query_tree.match(document):
                return document

    def sql_query(self) -> str:
        select_query = ['SELECT', '_data', 'FROM', self._table_name]
        where_clause = self._query_tree.sql(self._indexed_fields)

        if where_clause is not None:
            select_query += ['WHERE', where_clause]

        if self._limit and not self._query_tree._selectors:
            select_query += ['LIMIT', str(self._limit)]

        return ' '.join(select_query)


class Collection:
    __slots__ = (
        '_db', '_formated_indexed_fields', '_indexed_fields',
        '_name', '_registered'
    )

    INDEX_TYPES = {
        str: 'TEXT',
        int: 'INTEGER',
        float: 'REAL',
    }

    def __init__(self, db, name: str, **kwargs) -> None:
        self._db = db
        self._name = name
        self._registered = kwargs.get('registered', False)
        self._indexed_fields = set(kwargs.get('indexed_fields', []))
        self._formated_indexed_fields = {
            '"' + field + '"' for field in self._indexed_fields
        }

    def _register(self):
        if self._registered:
            return

        create_collection_query = (
            'CREATE TABLE {} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
            '_data BLOB NOT NULL);'
        ).format(self._name)
        self._db._connection.execute(create_collection_query)

        self._db._connection.execute(
            'INSERT INTO plume_master(collection_name)'
            ' VALUES (?)', (self._name,)
        )
        self._db._connection.commit()
        self._db._collections[self._name] = self
        self._registered = True

    def create_index(self, index: dict) -> None:
        if not self._registered:
            self._register()

        # Create a dedicated column for each indexed field.
        # We add double quotes around index and table names
        # for sqlite to allow us to use "."
        formated_indexed_field = []
        create_column_query = 'ALTER TABLE {} ADD COLUMN {} {}'
        for field, _type in index.items():
            field = '"' + field + '"'
            formated_indexed_field.append(field)
            self._db._connection.execute(
                create_column_query.format(
                    self._name,
                    field,
                    self.INDEX_TYPES[_type]
                )
            )

        # Create the Index on the column previously created.
        index_name = '_'.join((self._name, 'index', *index.keys()))
        csv_fields = ', '.join(formated_indexed_field)
        create_index = 'CREATE INDEX {} ON {}({})'.format(
            '"' + index_name + '"',
            self._name,
            csv_fields
        )
        self._db._connection.execute(create_index)

        # Register the new index on the master table for given table.
        for field in index.keys():
            self._indexed_fields.add(field)

        for field in formated_indexed_field:
            self._formated_indexed_fields.add(field)

        json_indexes = json.dumps(list(self._indexed_fields))
        update_master = (
            'UPDATE plume_master SET indexed_fields = ? '
            'WHERE collection_name = "{}"'
        ).format(self._name)
        self._db._connection.execute(update_master, [json_indexes])

    def find(self, query: dict, projection: dict=None,
             limit: int=None) -> list:
        if not self._registered:
            self._register()

        select_query = SelectQuery(
            self._name, self._indexed_fields, query, limit
        )
        sql_query = select_query.sql_query()
        result = self._db._connection.execute(sql_query).fetchall()
        documents = (json.loads(row[0]) for row in result)

        return select_query.match_many(documents)

    def find_one(self, query: dict, projection: dict=None):
        if not self._registered:
            self._register()

        select_query = SelectQuery(
            self._name, self._indexed_fields, query, 1
        )
        sql_query = select_query.sql_query()
        result = self._db._connection.execute(sql_query).fetchall()
        documents = (json.loads(row[0]) for row in result)
        return select_query.match_one(documents)

    def insert_one(self, document: dict) -> None:
        if not self._registered:
            self._register()

        fields = ['_data']
        values = [json.dumps(document)]
        placeholders = ['?']

        if self._indexed_fields:
            fields += self._formated_indexed_fields
            values += (
                _get_nested_value(document, field)
                for field in self._indexed_fields
            )
            placeholders += (len(self._formated_indexed_fields) * ['?'])

        insert_one_query = 'INSERT INTO {}({}) VALUES ({})'.format(
            self._name,
            ', '.join(fields),
            ', '.join(placeholders)
        )
        self._db._connection.execute(insert_one_query, values)
        self._db._connection.commit()

    def insert_many(self, documents: list) -> None:
        if not self._registered:
            self._register()

        fields = ['_data']
        placeholders = ['?']
        rows = []
        if self._indexed_fields:
            fields += list(self._formated_indexed_fields)
            placeholders += (len(self._formated_indexed_fields) * ['?'])

        for document in documents:
            row = [json.dumps(document)]
            if self._indexed_fields:
                row += (
                    _get_nested_value(document, field)
                    for field in self._indexed_fields
                )
            rows.append(row)

        insert_query = 'INSERT INTO {}({}) VALUES ({})'.format(
            self._name,
            ', '.join(fields),
            ', '.join(placeholders)
        )
        self._db._connection.executemany(insert_query, rows)
        self._db._connection.commit()


class PlumeDB:
    __slots__ = ('_collections', '_connection', '_db_name',)

    def __init__(self, db_name: str) -> None:
        self._db_name = db_name
        self._collections = {}
        self._connection = sqlite3.connect(self._db_name)

        self._connection.execute(
            'CREATE TABLE IF NOT EXISTS plume_master ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
            'collection_name TEXT UNIQUE NOT NULL,'
            'indexed_fields TEXT DEFAULT "[]")'
        )

        collections = self._connection.execute(
            'SELECT collection_name, indexed_fields FROM plume_master;'
        ).fetchall()

        for collection_name, indexed_fields in collections:
            collection = Collection(
                self,
                collection_name,
                indexed_fields=indexed_fields,
                registered=True,
            )
            self._collections[collection_name] = collection

    def __getattr__(self, collection_name: str) -> Collection:
        try:
            return self._collections[collection_name]
        except KeyError:
            return Collection(self, collection_name)
