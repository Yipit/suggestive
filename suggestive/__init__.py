from __future__ import unicode_literals
from collections import defaultdict
from itertools import chain


def expand(phrase):
    """Turns strings like this:

        >>> data = "Lincoln"

    Into this:

        >>> expand(data)
        ['l', 'li', 'linc', 'linco', 'lincol', 'lincoln']
    """
    return list(chain.from_iterable(
        [word[0:index + 1] for index, sub in enumerate(word)]
        for word in phrase.lower().split()))


class DummyBackend(object):
    def __init__(self):
        self._documents = {}
        self._terms = defaultdict(list)

    def documents(self):
        return self._documents

    def index(self, data_source, field):
        count = 0
        for doc in data_source:
            doc_id = doc['id']
            self._documents[doc_id] = doc
            for term in expand(doc[field]):
                self._terms[term].append(doc_id)
            count += 1

        return count

    def query(self, term, field, sort):
        result = []
        documents = self.documents()
        for doc_id in self._terms[term.lower()]:
            result.append(documents[doc_id])
        if sort:
            result.sort(key=lambda x: x[sort])
        return result


class Suggestive(object):

    def __init__(self, db, backend):
        self.db = db
        self.backend = backend

    def index(self, data_source, field):
        self.backend.index(data_source, field)

    def suggest(self, term, field, sort=None):
        return self.backend.query(term, field, sort)
