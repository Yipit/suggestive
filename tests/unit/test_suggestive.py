# -*- coding: utf-8; -*-
from __future__ import unicode_literals
import suggestive


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


def test_dummy_backend():
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
