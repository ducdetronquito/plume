Plume
=====

.. image:: https://img.shields.io/badge/license-public%20domain-ff69b4.svg
    :target: https://github.com/ducdetronquito/plume#license

.. image:: https://img.shields.io/badge/coverage-95%25-green.svg
    :target: #

.. image:: https://travis-ci.org/ducdetronquito/plume.svg?branch=master
     :target: https://travis-ci.org/ducdetronquito/plume


Outline
~~~~~~~

1. `Overview <https://github.com/ducdetronquito/plume#overview>`_
2. `Benefits <https://github.com/ducdetronquito/plume#benefits>`_
3. `Installation <https://github.com/ducdetronquito/plume#installation>`_
4. `Usage <https://github.com/ducdetronquito/plume#usage>`_
5. `License <https://github.com/ducdetronquito/plume#license>`_


Overview
~~~~~~~~

**Plume** is a small library that allows you to use SQLite3 as a document-oriented database.


Benefits
~~~~~~~~

Inspired from `Goatfish <https://github.com/skorokithakis/goatfish>`_, **Plume** provides:

* 😍 Concise API similar to MongoDB's API.
* 🔒 ACID transactions thanks to SQLite3.
* 💡 Complex queries with and/or operators and projections.
* ⚡ Indexes on collection, even on nested fields.
* 👌 Extended test suite.


Installation
~~~~~~~~~~~~

**Plume** is a Python3-only module that you will be soon able to install via ``pip``.
For now, as the library is still pretty unstable so you need to clone the repository.
    

The test suite (~78 tests) can be run with *pytest*

.. code:: sh

    pytest ./test


Usage
~~~~~

To play with Plume you need first to import the library and create a database.

.. code:: python
    
    from plume import Database
    
    db = Database('data.db')


You can access a collection like an attribute of your database instance.
If you access a collection that does not exists yet, it will be created on
the first read or write operation.

.. code:: python

    # The "actors" collection does not exists yet...
    actors = db.actors
    # ... but it is create on the fly on the first operation
    actors.find({'name': 'Benedict Cumberbatch'})


You can insert one or multiple `dict` documents to your collection.

.. code:: python
    
    db.actors.insert_one({
        'name': 'Bakery Cumbersome'
        'age': 41,
        'friends': ['John Watson', 'Jim Moriarty'],
        'social_media': {
            'mastodon': {
                'profile': 'Bakery@Cumbersome',
                'followers': 10,
            }
        }
    })
    
    # Consider a bulk insert if you have many documents
    db.actors.insert_many([{...}, {...}, {...}])


You can also make query to retrieve your documents, with comparison operators
(*$eq*, *$lt*, *$lte*, *$gt*, *$gte*, *$ne*) or logical operators (*$and*, *$or*).

.. code:: python
    
    # Retrieve actors that are between 18 and 42 years old
    # or named 'Beezlebub Cabbagepatch'.
    db.actors.find({
        '$or': [
            {'age': {'$gt': 18, '$lt': 42}},
            {'name': {'$eq': 'Beezlebub Cabbagepatch'},
        ]
    })


To retrieve only specific fields, you can specify a projection that describes fields to include or exclude.

.. code:: python

    # Retrieve only the name of actors that are more than 18 years old.
    db.actors.find(
        {'age': {'$gt': 18}},
        {'name': 1}
    )

The good part is that you can make query and projection on nested fields 👌.

.. code:: python

    # Retrieve only the mastodon profile of actors having more than
    # 42 mastodon followers.
    db.actors.find(
        {'social_media.mastodon.followers': {'$gt': 42}},
        {'social_media.mastodon.profile': 1}
    )

You can also retrieve a specific number of document by providing a *limit*...

.. code:: python
    
    #Retrieve 42 actors that are more than 18 years old.
    db.actors.find(
        {'name': {'$gt': 18}},
        {},
        42
    )


...but you can also retrieve a single document.

.. code:: python

    db.actors.find_one(
        {'age': {'$gt': 18}},
        {'name': 1}
    )


License
~~~~~~~

**Plume** is released into the **Public Domain**. 🎉

Ps: If we meet some day, and you think this small stuff worths it, you
can give me a beer, a coffee or a high-five in return: I would be really
happy to share a moment with you ! 🍻
