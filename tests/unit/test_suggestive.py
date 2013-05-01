# -*- coding: utf-8; -*-
from __future__ import unicode_literals
from mock import Mock, call, patch

import suggestive
import json


def test_suggestive():
    s = suggestive.Suggestive('names', backend=suggestive.DummyBackend())

    # Given that I have a source registered in my api
    data = [
        {'id': 23, 'name': 'Fafá de Belém'},
        {'id': 12.5, 'name': 'Fábio Júnior'},
        {'id': 20000, 'name': 'Fábio'},
    ]

    # When I index this source
    s.index(data, field='name', score='id')

    # Then I see that I can filter my results using the `term` parameter of the
    # `suggest()` method
    s.suggest('Faf').should.equal([
        {"id": 23, "name": "Fafá de Belém"},
    ])

    # And I also see that I can sort my results
    s.suggest('F').should.equal([
        {'id': 12.5, 'name': 'Fábio Júnior'},
        {'id': 23, 'name': 'Fafá de Belém'},
        {'id': 20000, 'name': 'Fábio'},
    ])

    # And I also see that I can limit my results
    s.suggest('F', limit=2, offset=1).should.equal([
        {'id': 23, 'name': 'Fafá de Belém'},
        {'id': 20000, 'name': 'Fábio'},
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
    indexed = backend.index(data, field='name', score='id')

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


def test_dummy_backend_cleaning_before_indexing():
    # Given that I have an instance of our dummy backend with some indexed data
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.DummyBackend()
    backend.index(data, field='name', score='name')

    # When I try to index the same documents but with different values
    data = [{'id': 0, 'name': 'Mingwei'}]
    backend.index(data, field='name', score='name')

    # Then I see that the terms were cleaned up too
    excluded_terms = [
        'l',
        'li',
        'lin',
        'linc',
        'linco',
        'lincol',
        'lincoln',
    ]

    for term in excluded_terms:
        backend._terms.should_not.contain(term)


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


def test_dummy_backend_remove_items():
    # Given that I have some indexed data
    data = [
        {"id": 0, "first_name": "Lincoln", "last_name": "Clarete"},
        {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
        {"id": 2, "first_name": "Livia", "last_name": "C"},
    ]
    backend = suggestive.DummyBackend()
    backend.index(data, field=['first_name', 'last_name'], score='id')

    # When I try to remove stuff
    backend.remove(0)

    # Then I see that we successfuly removed the first document
    backend.documents().should.equal(
        {1: {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
         2: {"id": 2, "first_name": "Livia", "last_name": "C"}},
    )

    # I also see that the terms for this document were removed
    dict(backend._terms).should.equal({
        'c': [2],
        'l': [2],
        'li': [2],
        'liv': [2],
        'livi': [2],
        'livia': [2],
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


def test_dummy_backend_query_limit():
    # Given that I have an instance of our dummy backend
    data = [
        {"id": 0, "name": "Lincoln"},
        {"id": 1, "name": "Livia"},
        {"id": 2, "name": "Linus"},
        {"id": 3, "name": "Lidia"},
    ]
    backend = suggestive.DummyBackend()
    backend.index(data, field='name', score='id')

    # Then I see that limit and offset are working properly with different
    # parameters
    backend.query('li', limit=1, offset=0).should.equal([
        {'id': 0, 'name': 'Lincoln'},
    ])
    backend.query('li', limit=1, offset=1).should.equal([
        {"id": 1, "name": "Livia"},
    ])
    backend.query('li', limit=2, offset=1).should.equal([
        {"id": 1, "name": "Livia"},
        {"id": 2, "name": "Linus"},
    ])

    # And I also see that the limit param works without the offset
    backend.query('li', limit=2).should.equal([
        {'id': 0, 'name': 'Lincoln'},
        {"id": 1, "name": "Livia"},
    ])

    # And I also see that the offset param works without the limit
    backend.query('li', offset=2).should.equal([
        {"id": 2, "name": "Linus"},
        {"id": 3, "name": "Lidia"},
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
    # Given that I have an instance of our redis backend
    conn = Mock()
    pipe = conn.pipeline.return_value
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.RedisBackend(conn=conn)
    backend.remove = Mock()     # We don't care about removing stuff here

    # When I try to index stuff
    indexed = backend.index(data, field='name', score='id')

    # Then I see that the number of indexed items is right
    indexed.should.equal(2)

    # And I see that the document set was fed
    list(conn.hset.call_args_list).should.equal([
        call('suggestive:d', 0, '{"id": 0, "name": "Lincoln"}'),
        call('suggestive:d', 1, '{"id": 1, "name": "Clarete"}')
    ])

    # And I see that we have a special stash to save to which terms each
    # document was added (it will make deletions way easier)
    list(pipe.sadd.call_args_list).should.equal([
        call('suggestive:dt:0', *suggestive.expand('lincoln')),
        call('suggestive:dt:1', *suggestive.expand('clarete')),
    ])

    # And the term set was also fed
    list(pipe.zadd.call_args_list).should.equal([
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

    # And I also see that the pipeline was executed!
    pipe.execute.assert_called_once_with()


def test_redis_backend_remove_items():
    # Given that I have some indexed data
    conn = Mock()
    pipe = conn.pipeline.return_value
    data = [
        {"id": 0, "first_name": "Lincoln", "last_name": "Clarete"},
        {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
        {"id": 2, "first_name": "Livia", "last_name": "C"},
    ]
    backend = suggestive.RedisBackend(conn=conn)

    with patch.object(backend, 'remove'):
        backend.index(data, field=['first_name', 'last_name'], score='id')

    # Mocking the term X doc cache set
    conn.smembers.return_value = (
        suggestive.expand('lincoln') + suggestive.expand('clarete'))

    # When I try to remove stuff
    backend.remove(0)

    # Then I see that the terms for this document were removed
    list(pipe.zrem.call_args_list).should.equal([
        call('suggestive:d:l', 0),
        call('suggestive:d:li', 0),
        call('suggestive:d:lin', 0),
        call('suggestive:d:linc', 0),
        call('suggestive:d:linco', 0),
        call('suggestive:d:lincol', 0),
        call('suggestive:d:lincoln', 0),
        call('suggestive:d:c', 0),
        call('suggestive:d:cl', 0),
        call('suggestive:d:cla', 0),
        call('suggestive:d:clar', 0),
        call('suggestive:d:clare', 0),
        call('suggestive:d:claret', 0),
        call('suggestive:d:clarete', 0),
    ])

    pipe.execute.call_count.should.equal(2)

    # And the cache key should also be removed
    conn.delete.assert_called_once_with('suggestive:dt:0')

    # And I also see that we successfuly removed the document too
    conn.hdel.assert_called_once_with('suggestive:d', 0)


def test_redis_backend_cleaning_before_indexing():
    # Given that I have an instance of our redis backend with some indexed data
    conn = Mock()
    data = [{"id": 0, "name": "Lincoln"}, {"id": 1, "name": "Clarete"}]
    backend = suggestive.RedisBackend(conn=conn)
    backend.remove = Mock()
    backend.index(data, field='name', score='name')

    # When I try to index the same documents but with different values
    data = [{'id': 0, 'name': 'Mingwei'}]
    backend.index(data, field='name', score='name')

    # Then I see that the remove method was called for every single document
    # that before indexing it
    list(backend.remove.call_args_list).should.equal([
        call(0),
        call(1),
        call(0),
    ])


def test_redis_backend_indexing_multiple_fields():
    # Given that I have an instance of our redis backend
    conn = Mock()
    pipe = conn.pipeline.return_value
    data = [
        {"id": 0, "first_name": "Lincoln", "last_name": "Clarete"},
        {"id": 1, "first_name": "Mingwei", "last_name": "Gu"},
    ]
    backend = suggestive.RedisBackend(conn=conn)

    # When I try to index stuff
    with patch.object(backend, 'remove'):
        backend.index(data, field=['first_name', 'last_name'], score='id')

    # And the term set was also fed
    list(pipe.zadd.call_args_list).should.equal([
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

    # And I also see that the pipeline was executed!
    pipe.execute.assert_called_once_with()


def test_redis_backend_querying():
    conn = Mock()
    data = [
        {"id": 0, "name": "Lincoln", "score": 33.3},
        {"id": 1, "name": "Livia", "score": 22.2},
        {"id": 5, "name": "Linus", "score": 25},  # Rita's brother! :)
    ]

    # Given that I have an instance of our Redis backend
    backend = suggestive.RedisBackend(conn=conn)
    with patch.object(backend, 'remove'):
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


def test_redis_backend_query_limit():
    # Given that I have an instance of our dummy backend
    data = [
        {"id": 0, "name": "Lincoln"},
        {"id": 1, "name": "Livia"},
        {"id": 2, "name": "Linus"},
        {"id": 3, "name": "Lidia"},
    ]
    conn = Mock()
    conn.hmget.return_value = []
    backend = suggestive.RedisBackend(conn=conn)
    with patch.object(backend, 'remove'):
        backend.index(data, field='name', score='id')

    # Then I see that limit and offset are working properly with different
    # parameters
    backend.query('li', limit=1, offset=0)
    conn.zrevrange.assert_called_once_with('suggestive:d:li', 0, 1)
    conn.reset_mock()

    backend.query('li', limit=1, offset=1)
    conn.zrevrange.assert_called_once_with('suggestive:d:li', 1, 2)
    conn.reset_mock()

    backend.query('li', limit=2, offset=1)
    conn.zrevrange.assert_called_once_with('suggestive:d:li', 1, 3)
    conn.reset_mock()

    # And I also see that the limit param works without the offset
    backend.query('li', limit=2)
    conn.zrevrange.assert_called_once_with('suggestive:d:li', 0, 2)
    conn.reset_mock()

    # And I also see that the offset param works without the limit
    backend.query('li', offset=2)
    conn.zrevrange.assert_called_once_with('suggestive:d:li', 2, -1)
    conn.reset_mock()


def test_both_backends_query_return_term_prefixed_words():
    # Given that I have an instance of our dummy backend with some data indexed
    data = [
        {"id": 0, 'field1': 'Pascal programming language', 'field2': 'Python'},
        {"id": 1, 'field1': 'Italian Paníni', 'field2': 'Pizza Italiana'},
        {"id": 2, 'field1': 'Pacific Ocean', 'field2': 'Posseidon, The king'},
        {"id": 3, 'field1': 'Kiwi', 'field2': 'Passion-Fruit'},
        {"id": 4, 'field1': 'I love', 'field2': 'Paníni'},
    ]
    conn = Mock()
    conn.zrevrange.return_value = range(4)
    conn.hmget.return_value = [json.dumps(item) for item in data]

    dummy_backend = suggestive.DummyBackend()
    dummy_backend.index(data, field=['field1', 'field2'], score='id')
    redis_backend = suggestive.RedisBackend(conn=conn)
    with patch.object(redis_backend, 'remove'):
        redis_backend.index(data, field=['field1', 'field2'], score='id')

    # When I query for the `Pa` prefix, asking for the words found in the
    # documents
    dummy_backend.query('pa', words=True).should.equal([
        'Pascal', 'Paníni', 'Pacific', 'Passion-Fruit'
    ])
    redis_backend.query('pa', words=True).should.equal([
        'Pascal', 'Paníni', 'Pacific', 'Passion-Fruit'
    ])
    suggestive.Suggestive('meh', backend=dummy_backend).suggest(
        'pa', words=True).should.equal([
            'Pascal', 'Paníni', 'Pacific', 'Passion-Fruit'
        ])


def test_suggestive_remove():
    # Given that we have an instance of suggestive with a fake backend
    backend = Mock()
    s = suggestive.Suggestive('stuff', backend=backend)

    # When I try to remove a document from the index
    s.remove(1)

    # Then I see that the backend remove() method was successfuly called
    backend.remove.assert_called_once_with(1)


def test_suggestive_unidecoded():
    # Given that I have an instance of suggestive
    s = suggestive.Suggestive('blah', backend=suggestive.DummyBackend())

    # When I index accented words
    data = [{'id': 0, 'name': 'Líncóln'}]
    s.index(data, field='name', score='id')

    # Then I see that suggestive will still suggest stuff when fed with a word
    # without accent
    s.suggest('li').should.equal([{'id': 0, 'name': 'Líncóln'}])
