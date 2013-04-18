from __future__ import unicode_literals
from collections import defaultdict
from itertools import chain

import json


def expand(phrase, min_chars=1):
    """Turns strings like this:

        >>> data = "Lincoln"

    Into this:

        >>> expand(data)
        ['l', 'li', 'linc', 'linco', 'lincol', 'lincoln']
    """
    base = list(chain.from_iterable(
        [word[0:index] for index, sub in enumerate(word, start=min_chars)]
        for word in phrase.lower().split()))
    result = set()
    return [x for x in base if x not in result and not result.add(x)]


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


class KeyManager(object):

    def for_docs(self):
        return 'suggestive:d'

    def for_term(self, term):
        return 'suggestive:d:{}'.format(term)


class RedisBackend(object):
    def __init__(self, conn=None):
        self.conn = conn
        self.keys = KeyManager()

    def documents(self):
        items = self.conn.hgetall(self.keys.for_docs()).items()
        return {doc_id: json.loads(doc) for doc_id, doc in items}

    def index(self, data_source, field):
        count = 0
        for doc in data_source:
            doc_id = doc['id']
            self.conn.hset(self.keys.for_docs(), doc_id, json.dumps(doc))
            for term in expand(doc[field]):
                self.conn.zadd(self.keys.for_term(term), 0, doc_id)
            count += 1
        return count

    def query(self, term, field, sort):
        doc_ids = self.conn.zrange(self.keys.for_term(term.lower()), 0, -1)
        docs = self.conn.hmget(self.keys.for_docs(), doc_ids)
        return [json.loads(d) for d in docs]


class Suggestive(object):

    def __init__(self, db, backend):
        self.db = db
        self.backend = backend

    def index(self, data_source, field):
        self.backend.index(data_source, field)

    def suggest(self, term, field, sort=None):
        return self.backend.query(term, field, sort)
