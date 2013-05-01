# -*- coding: utf-8; -*-
from __future__ import unicode_literals
from sure import scenario

import redis
import suggestive


def connect(context):
    """Prepare the redis connection before each test

    This helper ensures that each test will be called with a proper redis
    connection and it will also flush redis, so be careful."""
    if not hasattr(context, 'conn'):
        context.conn = redis.StrictRedis()
    context.conn.flushdb()
    return context


@scenario(connect)
def test_redis_backend_indexing(context):
    # Given that I have an instance of our dummy backend
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.RedisBackend(conn=context.conn)

    # When I try to index stuff
    indexed = backend.index(data, field='name', score='id')

    # Then I see that the number of indexed items is right
    indexed.should.equal(2)


@scenario(connect)
def test_redis_backend_remove_items(context):
    # Given that I have some indexed data
    data = [
        {"id": 0, "first_name": "Lincoln", "last_name": "Clarete"},
        {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
        {"id": 2, "first_name": "Livia", "last_name": "C"},
    ]
    backend = suggestive.RedisBackend(conn=context.conn)
    backend.index(data, field=['first_name', 'last_name'], score='id')

    # When I try to remove stuff
    backend.remove(0)

    # Then I see that the terms that also occour in other docs are still
    # there. But we got rid of the document 0
    context.conn.zrange('suggestive:d:l', 0, -1).should.equal(['2'])
    context.conn.zrange('suggestive:d:li', 0, -1).should.equal(['2'])

    # And I see that the terms for this document were removed
    context.conn.exists('suggestive:d:lin').should.be.false
    context.conn.exists('suggestive:d:linc').should.be.false
    context.conn.exists('suggestive:d:linco').should.be.false
    context.conn.exists('suggestive:d:lincol').should.be.false
    context.conn.exists('suggestive:d:lincoln').should.be.false

    # And the cache key should also be removed
    context.conn.exists('suggestive:dt:0').should.be.false

    # And I also see that we successfuly removed the document too
    context.conn.hexists('suggestive:d', 0).should.be.false


@scenario(connect)
def test_redis_backend_querying(context):
    data = [
        {"id": 0, "name": "Lincoln", "score": 33.3},
        {"id": 1, "name": "Livia", "score": 22.2},
        {"id": 5, "name": "Linus", "score": 25},  # Rita's brother! :)
    ]

    # Given that I have an instance of our Redis backend
    backend = suggestive.RedisBackend(conn=context.conn)
    backend.index(data, field='name')  # We'll choose `score` by default

    # When I try to query stuff sorting by score (defaults to asc), Then I see
    # it worked
    backend.query('li').should.equal([
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 1, "name": 'Livia', 'score': 22.2},
    ])

    # And I see that the 'reversed' version also works
    backend.query('li', reverse=True).should.equal([
        {"id": 1, "name": 'Livia', 'score': 22.2},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
    ])


@scenario(connect)
def test_redis_backend_cleaning_before_indexing(context):
    # Given that I have an instance of our redis backend with some indexed data
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.RedisBackend(conn=context.conn)
    backend.index(data, field='name', score='id')

    # When I try to index the same documents but with different values
    data = [{'id': 0, 'name': 'Mingwei'}]
    backend.index(data, field='name', score='id')

    # Then I see that the remove method was called for every single document
    # that before indexing it
    context.conn.smembers(backend.keys.for_cache(0)).should.equal(set([
        'm',
        'mi',
        'min',
        'ming',
        'mingw',
        'mingwe',
        'mingwei',
    ]))


@scenario(connect)
def test_suggestive(context):
    s = suggestive.Suggestive(
        'names',
        backend=suggestive.RedisBackend(conn=context.conn))

    # Given that I have a source registered in my api
    data = [
        {"id": 0, "name": "Fafá de Belém", "score": 23},
        {"id": 1, "name": "Fábio Júnior", "score": 12.5},
        {"id": 2, "name": "Fábio", "score": 20000},
    ]

    # When I index this source
    s.index(data, field='name')

    # Then I see that I can filter my results using the `term` parameter of the
    # `suggest()` method
    s.suggest('Faf').should.equal([
        {"id": 0, "name": "Fafá de Belém", "score": 23},
    ])

    # And I also see that I can sort my results
    s.suggest('F').should.equal([
        {"id": 2, "name": "Fábio", "score": 20000},
        {"id": 0, "name": "Fafá de Belém", "score": 23},
        {"id": 1, "name": "Fábio Júnior", "score": 12.5},
    ])
