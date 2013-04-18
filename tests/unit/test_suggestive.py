# -*- coding: utf-8; -*-
from __future__ import unicode_literals
import suggestive

from mock import Mock


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
    s.suggest('Fa', field='name').should.equal([
        {"id": 0, "name": "Fafá de Belém", "score": 23},
    ])

    # And I also see that I can sort my results
    s.suggest('F', field='name', sort='score').should.equal([
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
    indexed = backend.index(data, field='name')

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


def test_dummy_backend_querying():
    # Given that I have an instance of our dummy backend
    data = [
        {"id": 0, "name": "Lincoln"},
        {"id": 1, "name": "Livia"},
        {"id": 5, "name": "Linus"},  # Rita's brother! :)
    ]
    backend = suggestive.DummyBackend()
    backend.index(data, field='name')

    # When I try to query stuff
    result = backend.query('lin', field='name', sort='name')

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
    backend.index(data, field='name')

    # When I try to query stuff sorting by score (defaults to asc), Then I see
    # it worked
    backend.query('li', field='name', sort='score').should.equal([
        {"id": 1, "name": 'Livia', 'score': 22.2},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
    ])

    # And I see that the 'reversed' version also works
    backend.query('li', field='name', sort='-score').should.equal([
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 1, "name": 'Livia', 'score': 22.2},
    ])

    # And I see that the sort also works for string fields
    backend.query('li', field='name', sort='name').should.equal([
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 1, "name": 'Livia', 'score': 22.2},
    ])

    # And I see that the sort also works for string fields reversed
    backend.query('li', field='name', sort='-name').should.equal([
        {"id": 1, "name": 'Livia', 'score': 22.2},
        {"id": 5, "name": 'Linus', 'score': 25},
        {"id": 0, "name": 'Lincoln', 'score': 33.3},
    ])


def test_redis_backend_indexing():
    # Given that I have an instance of our dummy backend
    conn = Mock()
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.RedisBackend(conn=conn)

    # When I try to index stuff
    indexed = backend.index(data, field='name')

    # Then I see that the number of indexed items is right
    indexed.should.equal(2)

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


def test_redis_backend_querying():
    # Given that I have an instance of our dummy backend
    conn = Mock()
    data = [
        {"id": 0, "name": "Lincoln"},
        {"id": 1, "name": "Livia"},
        {"id": 5, "name": "Linus"},  # Rita's brother! :)
    ]
    backend = suggestive.RedisBackend(conn=conn)
    backend.index(data, field='name')

    # When I try to query stuff
    conn.zrange.return_value = ['0', '5']
    conn.hmget.return_value = [
        '{"id": 0, "name": "Lincoln"}',
        '{"id": 5, "name": "Linus"}',
    ]
    result = backend.query('lin', field='name', sort=None)

    # Then I see the result is correct
    result.should.equal([
        {'id': 0, 'name': 'Lincoln'},
        {"id": 5, "name": 'Linus'},
    ])
