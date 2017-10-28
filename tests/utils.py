# Built-in dependencies
import os

# Internal dependencies
from plume import Database
from factories import Persona


class WritingBaseTest:
    def setup(self):
        self.db = Database('test.db')

    def teardown(self):
        os.remove('test.db')


class ReadingBaseTest:
    def setup_class(cls):
        cls.db = Database('test.db')

    def teardown_class(self):
        os.remove('test.db')


def collection_is_registered(db, collection_name):
    query = (
        "SELECT name FROM sqlite_master WHERE "
        "type='table' AND name='{}';"
    ).format(collection_name)

    result = db._connection.execute(query).fetchone()
    return result


def table_info(db, table_name):
    query = 'PRAGMA table_info({})'.format(table_name)
    return db._connection.execute(query).fetchall()


def index_list(db, table_name):
    query = 'PRAGMA index_list({})'.format(table_name)
    return db._connection.execute(query).fetchall()


def with_index(**decorator_params):
    def with_index_decorator(fun):
        def inner_with_index(*args, **kwargs):
            test_instance = args[0]
            index = decorator_params['index']
            test_instance.db.personas.create_index(index)
            return fun(*args, **kwargs)
        return inner_with_index
    return with_index_decorator


def with_documents(**decorator_params):
    def with_documents_decorator(fun):
        def inner_with_documents(*args, **kwargs):
            test_instance = args[0]
            number = decorator_params['number']
            documents = Persona.create_batch(number)
            if isinstance(documents, list):
                test_instance.db.personas.insert_many(documents)
                test_instance.personas = documents
            elif documents:
                test_instance.db.personas.insert_one(documents)
                test_instance.persona = documents
            return fun(*args, **kwargs)
        return inner_with_documents
    return with_documents_decorator
