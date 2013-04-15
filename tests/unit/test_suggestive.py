# -*- coding: utf-8; -*-
import suggestive


def test_basic_source_answer():
    s = suggestive.Suggestive('names', backend=suggestive.DummyBackend())

    # Given that I have a source registered in my api
    data = [
        {"name": "Fafá de Belém", "score": 23},
        {"name": "Fábio Júnior", "score": 12.5},
        {"name": "Fábio", "score": 20000},
    ]

    # When I index this source
    s.index(data)

    # Then I see that I can filter my results using the `term` parameter of the
    # `suggest()` method
    s.suggest('Fa', field='name').should.equal([
        {"name": "Fafá de Belém", "score": 23},
    ])

    # And I also see that I can sort my results
    s.suggest('F', field='name', sort='score').should.equal([
        {"name": "Fábio Júnior", "score": 12.5},
        {"name": "Fafá de Belém", "score": 23},
        {"name": "Fábio", "score": 20000},
    ])
