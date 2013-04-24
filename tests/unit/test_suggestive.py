# -*- coding: utf-8; -*-
from __future__ import unicode_literals
import suggestive

from mock import Mock, call


def test_suggestive():
    s = suggestive.Suggestive('names', backend=suggestive.DummyBackend())

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
    s.suggest('Fa').should.equal([
        {"id": 0, "name": "Fafá de Belém", "score": 23},
    ])

    # And I also see that I can sort my results
    s.suggest('F').should.equal([
        {"id": 1, "name": "Fábio Júnior", "score": 12.5},
        {"id": 0, "name": "Fafá de Belém", "score": 23},
        {"id": 2, "name": "Fábio", "score": 20000},
    ])


def test_expand():
    suggestive.expand("Lincoln Clarete").should.equal([
        'l', 'li', 'lin', 'linc', 'linco', 'lincol', 'lincoln',
        'c', 'cl', 'cla', 'clar', 'clare', 'claret', 'clarete',
    ])

    suggestive.expand("Lincoln", min_chars=2).should.equal([
        'li', 'lin', 'linc', 'linco', 'lincol', 'lincoln',
    ])


def test_dummy_backend_indexing():
    # Given that I have an instance of our dummy backend
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.DummyBackend()

    # When I try to index stuff
    indexed = backend.index(data, field='name', score='name')

    # Then I see that the number of indexed items is right
    indexed.should.equal(2)

    # And that all the documents are indexed
    backend.documents().should.equal({
        0: {u'id': 0, u'name': u'Lincoln'},
        1: {u'id': 1, u'name': u'Clarete'}
    })

    # And that the cache contains all indexed fields
    dict(backend._terms).should.equal({
        'c': [1],
        'cl': [1],
        'cla': [1],
        'clar': [1],
        'clare': [1],
        'claret': [1],
        'clarete': [1],
        'l': [0],
        'li': [0],
        'lin': [0],
        'linc': [0],
        'linco': [0],
        'lincol': [0],
        'lincoln': [0],
    })


def test_dummy_backend_indexing_multiple_fields():
    # Given that I have some data to index
    data = [
        {"id": 0, "first_name": "Lincoln", "last_name": "Clarete"},
        {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
    ]
    backend = suggestive.DummyBackend()

    # When I try to index stuff
    backend.index(data, field=['first_name', 'last_name'], score='id')

    # And that the cache contains all indexed fields
    dict(backend._terms).should.equal({
        'c': [0],
        'cl': [0],
        'cla': [0],
        'clar': [0],
        'clare': [0],
        'claret': [0],
        'clarete': [0],
        'l': [0],
        'li': [0],
        'lin': [0],
        'linc': [0],
        'linco': [0],
        'lincol': [0],
        'lincoln': [0],
        'm': [1],
        'mi': [1],
        'min': [1],
        'ming': [1],
        'mingw': [1],
        'mingwe': [1],
        'mingwei': [1],
        'g': [1],
        'gu': [1],
    })


def test_dummy_backend_querying():
    # Given that I have an instance of our dummy backend
    data = [
        {"id": 0, "name": "Lincoln"},
        {"id": 1, "name": "Livia"},
        {"id": 5, "name": "Linus"},  # Rita's brother! :)
    ]
    backend = suggestive.DummyBackend()
    backend.index(data, field='name', score='id')

    # When I try to query stuff
    result = backend.query('lin')

    # Then I see the result is correct
    result.should.equal([
        {'id': 0, 'name': 'Lincoln'},
        {"id": 5, "name": "Linus"},
    ])


def test_dummy_backend_query_sorting():
    # Given that I have an instance of our dummy backend
    data = [
        {"id": 0, "name": "Lincoln", "score": 33.3},
        {"id": 1, "name": "Livia", "score": 22.2},
        {"id": 5, "name": "Linus", "score": 25},  # Rita's brother! :)
    ]
    backend = suggestive.DummyBackend()
    backend.index(data, field='name', score='score')

    # When I try to query stuff sorting by score (defaults to asc), Then I see
    # it worked
    backend.query('li').should.equal([
        {"id": 1, "name": 'Livia', 'score': 22.2},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
    ])

    # And I see that the 'reversed' version also works
    backend.query('li', reverse=True).should.equal([
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 1, "name": 'Livia', 'score': 22.2},
    ])


def test_redis_backend_indexing():
    # Given that I have an instance of our dummy backend
    conn = Mock()
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.RedisBackend(conn=conn)

    # When I try to index stuff
    indexed = backend.index(data, field='name', score='id')

    # Then I see that the number of indexed items is right
    indexed.should.equal(2)

    # And I see that the document set was fed
    list(conn.hset.call_args_list).should.equal([
        call('suggestive:d', 0, '{"id": 0, "name": "Lincoln"}'),
        call('suggestive:d', 1, '{"id": 1, "name": "Clarete"}')
    ])

    # And the term set was also fed
    list(conn.zadd.call_args_list).should.equal([
        call('suggestive:d:l', 0, 0),
        call('suggestive:d:li', 0, 0),
        call('suggestive:d:lin', 0, 0),
        call('suggestive:d:linc', 0, 0),
        call('suggestive:d:linco', 0, 0),
        call('suggestive:d:lincol', 0, 0),
        call('suggestive:d:lincoln', 0, 0),
        call('suggestive:d:c', 1, 1),
        call('suggestive:d:cl', 1, 1),
        call('suggestive:d:cla', 1, 1),
        call('suggestive:d:clar', 1, 1),
        call('suggestive:d:clare', 1, 1),
        call('suggestive:d:claret', 1, 1),
        call('suggestive:d:clarete', 1, 1)
    ])

    # And that all the documents are indexed
    conn.hgetall.return_value = {
        '0': '{"id": 0, "name": "Lincoln"}',
        '1': '{"id": 1, "name": "Clarete"}',
    }
    backend.documents().should.equal({
        '0': {u'id': 0, u'name': u'Lincoln'},
        '1': {u'id': 1, u'name': u'Clarete'}
    })
    conn.hgetall.assert_called_once_with('suggestive:d')


def test_redis_backend_indexing_multiple_fields():
    # Given that I have an instance of our dummy backend
    conn = Mock()
    data = [
        {"id": 0, "first_name": "Lincoln", "last_name": "Clarete"},
        {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
    ]
    backend = suggestive.RedisBackend(conn=conn)

    # When I try to index stuff
    backend.index(data, field=['first_name', 'last_name'], score='id')

    # And the term set was also fed
    list(conn.zadd.call_args_list).should.equal([
        call('suggestive:d:l', 0, 0),
        call('suggestive:d:li', 0, 0),
        call('suggestive:d:lin', 0, 0),
        call('suggestive:d:linc', 0, 0),
        call('suggestive:d:linco', 0, 0),
        call('suggestive:d:lincol', 0, 0),
        call('suggestive:d:lincoln', 0, 0),
        call('suggestive:d:c', 0, 0),
        call('suggestive:d:cl', 0, 0),
        call('suggestive:d:cla', 0, 0),
        call('suggestive:d:clar', 0, 0),
        call('suggestive:d:clare', 0, 0),
        call('suggestive:d:claret', 0, 0),
        call('suggestive:d:clarete', 0, 0),
        call('suggestive:d:m', 1, 1),
        call('suggestive:d:mi', 1, 1),
        call('suggestive:d:min', 1, 1),
        call('suggestive:d:ming', 1, 1),
        call('suggestive:d:mingw', 1, 1),
        call('suggestive:d:mingwe', 1, 1),
        call('suggestive:d:mingwei', 1, 1),
        call('suggestive:d:g', 1, 1),
        call('suggestive:d:gu', 1, 1),
    ])


def test_redis_backend_querying():
    conn = Mock()
    data = [
        {"id": 0, "name": "Lincoln", "score": 33.3},
        {"id": 1, "name": "Livia", "score": 22.2},
        {"id": 5, "name": "Linus", "score": 25},  # Rita's brother! :)
    ]

    # Given that I have an instance of our Redis backend
    backend = suggestive.RedisBackend(conn=conn)
    backend.index(data, field='name')  # We'll choose `score` by default
    conn.zrevrange.return_value = ['1', '5', '0']
    conn.zrange.return_value = ['0', '5', '1']

    # When I try to query stuff sorting by score (defaults to asc), Then I see
    # it worked
    conn.hmget.return_value = [
        '{"id": 0, "name": "Lincoln", "score": 33.3}',
        '{"id": 5, "name": "Linus", "score": 25}',
        '{"id": 1, "name": "Livia", "score": 22.2}',
    ]
    backend.query('li').should.equal([
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 1, "name": 'Livia', 'score': 22.2},
    ])
    conn.zrevrange.assert_called_once_with(
        'suggestive:d:li', 0, -1)

    # And I see that the 'reversed' version also works
    conn.hmget.return_value = [
        '{"id": 1, "name": "Livia", "score": 22.2}',
        '{"id": 5, "name": "Linus", "score": 25}',
        '{"id": 0, "name": "Lincoln", "score": 33.3}',
    ]
    backend.query('li', reverse=True).should.equal([
        {"id": 1, "name": 'Livia', 'score': 22.2},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
    ])
    conn.zrange.assert_called_once_with(
        'suggestive:d:li', 0, -1)


def test_redis_backend_querying_without_indexing():
    conn = Mock()
    backend = suggestive.RedisBackend(conn=conn)

    # Given that I have an empty result set
    conn.hmget.side_effect = Exception('hmget shouldn\'t be called!')
    conn.zrevrange.return_value = []

    # When I query for something that is not indexed
    backend.query('li').should.equal([])

    # Then I see that I look for the right term
    conn.zrevrange.assert_called_once_with(
        'suggestive:d:li', 0, -1)

    # And since no document was found, we didn't try to get them from redis.
    conn.hmget.called.should.be.false
