# -*- coding: utf-8; -*-
from __future__ import unicode_literals
from collections import defaultdict
from itertools import chain

import re
import six
import json


__version__ = '0.0.2'


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


def find_words_in_doc(doc, term):
    sub = []
    values = [v for v in doc.values() if isinstance(v, six.string_types)]
    for value in values:
        words = re.findall('[\w-]+', value, re.I | re.U)
        sub.extend(w for w in words if w.lower().startswith(term))
    return list(sub)


class DummyBackend(object):
    def __init__(self):
        self._documents = {}
        self._terms = defaultdict(list)

    def documents(self):
        return self._documents

    def index(self, data, field, score='score'):
        count = 0
        for doc in sorted(data, key=lambda x: x[score]):
            doc_id = doc['id']
            self._documents[doc_id] = doc
            for f in isinstance(field, list) and field or [field]:
                for term in expand(doc[f]):
                    self._terms[term].append(doc_id)
            count += 1
        return count

    def query(self, term, reverse=False, words=False, limit=-1, offset=0):
        result = []
        term = term.lower()
        documents = self.documents()

        for doc_id in self._terms[term]:
            if words:
                result.extend(
                    w for w in find_words_in_doc(documents[doc_id], term)
                    if w not in result)
            else:
                result.append(documents[doc_id])
        if reverse:
            result.reverse()
        return result[offset:limit >= 0 and (offset + limit) or None]


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

    def index(self, data_source, field, score='score'):
        count = 0
        pipe = self.conn.pipeline()
        for doc in data_source:
            doc_id = doc['id']
            self.conn.hset(self.keys.for_docs(), doc_id, json.dumps(doc))
            for f in isinstance(field, list) and field or [field]:
                for term in expand(doc[f]):
                    pipe.zadd(
                        self.keys.for_term(term), doc[score], doc_id)
            count += 1
        pipe.execute()
        return count

    def query(self, term, reverse=False, words=False, limit=-1, offset=0):
        doc_ids = (self.conn.zrevrange if not reverse else self.conn.zrange)(
            self.keys.for_term(term.lower()),
            offset,
            limit >= 0 and (offset + limit) or -1)

        result = []
        docs = doc_ids and self.conn.hmget(self.keys.for_docs(), doc_ids) or []
        for d in docs:
            doc = json.loads(d)
            if words:
                result.extend(
                    w for w in find_words_in_doc(doc, term)
                    if w not in result)
            else:
                result.append(doc)

        return result


class Suggestive(object):

    def __init__(self, db, backend):
        self.db = db
        self.backend = backend

    def index(self, data_source, field):
        self.backend.index(data_source, field)

    def suggest(self, term, words=False, limit=-1, offset=0):
        return self.backend.query(
            term, words=words, limit=limit, offset=offset)
