# -*- coding: utf-8; -*-
from __future__ import unicode_literals
from collections import defaultdict
from itertools import chain
from unidecode import unidecode

import re
import six
import json


__version__ = '0.2.0'


def expand(phrase, min_chars=1):
    """Turns strings like this:

        >>> data = "Lincoln"

    Into this:

        >>> expand(data)
        ['l', 'li', 'linc', 'linco', 'lincol', 'lincoln']
    """
    base = list(chain.from_iterable(
        [word[0:index] for index, sub in enumerate(word, start=min_chars)]
        for word in unidecode(phrase.lower()).split()))
    result = set()
    return [x for x in base if x not in result and not result.add(x)]


def find_words_in_doc(doc, term):
    """Walk through a dict `doc` and find all words that start with `term`

    Just like this:

        >>> doc = {'blah': 'Rocky Balboa', 'bleh': 'We love rock!'}
        >>> find_words_in_doc(doc, 'ro')
        ['Rocky', 'rock']
    """
    sub = []
    values = [v for v in doc.values() if isinstance(v, six.string_types)]
    for value in values:
        words = re.findall('[\w-]+', value, re.I | re.U)
        sub.extend(w for w in words if w.lower().startswith(term))
    return list(sub)


class DummyBackend(object):
    """Reference implementation for all new features

    This dummy backend has two main goals:

      1. it is easier to prototype new features using python data types than in
         redis;

      2. it should be safe to use this backend when you need to write unit
         tests that use suggestive;

    To use this class, you just need to instantiate the `Suggestive` class
    passing an instance of this class to the `backend` argument. Just like
    this:

        >>> import suggestive
        >>> s = suggestive.Suggestive('db', backend=suggestive.DummyBackend())
        >>> s.index([{'id': 0, 'name': 'Lincoln'}], field='name', score='id')
        >>> s.suggest('li')
        [{'id': 0, 'name': 'Lincoln'}]

    Boom!
    """
    def __init__(self):
        self._documents = {}
        self._terms = defaultdict(list)

    def documents(self):
        """Return all indexed documents"""
        return self._documents

    def index(self, data, field, score='score'):
        """Index a list of documents

        Before receiving suggestions, you need to feed a database with all the
        documents that suggestive will manage. The `data` field is an iterable
        containing dictionaries that _must_ have the same keys. IOW, the code
        below will raise a `KeyError`:

            >>> index([{'id': 0, 'name': 'Lincoln'}, {'id': 1}], field='name')

        The param `field` is a `str` or a `list` of `str` that informs which
        keys of the document should be indexed. For example:

            >>> index([{'id': 0, 'name': 'Lincoln Clarete'}], field='name')

        The code above will index the words 'Lincoln' and 'Clarete'. The param
        `score` informs which field should be used to sort the documents.

        Both `field` and `score` keys are *REQUIRED* in all docs you try to
        index. You can use a different key name for the `score` field. This
        way, the following example is still valid:

            >>> index([{'id': 0, 'name': 'Lincoln'}], field='name', score='id')
        """
        count = 0
        for doc in sorted(data, key=lambda x: x[score]):
            doc_id = doc['id']
            self.remove(doc_id)
            self._documents[doc_id] = doc
            for f in isinstance(field, list) and field or [field]:
                for term in expand(doc[f]):
                    self._terms[term].append(doc_id)
            count += 1
        return count

    def remove(self, doc_id):
        """Remove a document from the storage and all its terms too

        This method is smart enough to don't cleanup terms used for more than
        one document.
        """
        # Cleaning up terms
        for term, docs in self._terms.items():
            if doc_id in docs:
                docs.remove(doc_id)
                if not docs:
                    del self._terms[term]

        # Cleaning up the actual document
        if doc_id in self._documents:
            del self._documents[doc_id]

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

    def for_cache(self, doc_id):
        return 'suggestive:dt:{}'.format(doc_id)


class RedisBackend(object):
    def __init__(self, conn=None):
        self.conn = conn
        self.keys = KeyManager()

    def documents(self):
        items = self.conn.hgetall(self.keys.for_docs()).items()
        return {doc_id: json.loads(doc) for doc_id, doc in items}

    def index(self, data_source, field, score='score'):
        count = 0
        pipe = self.conn.pipeline(transaction=False)
        for doc in data_source:
            doc_id = doc['id']
            self.remove(doc_id)
            self.conn.hset(self.keys.for_docs(), doc_id, json.dumps(doc))
            for f in isinstance(field, list) and field or [field]:

                # All possible terms for the fields we're analyzing right now.
                terms = expand(doc[f])

                # Caching which documents were related to which terms. It will
                # speed up the removal process of a document from its terms a
                # lot.
                pipe.sadd(self.keys.for_cache(doc_id), *terms)

                # Time to add the documents to the terms expanded from the
                # indexable fields.
                for term in terms:
                    pipe.zadd(
                        self.keys.for_term(term), doc[score], doc_id)
            count += 1
        pipe.execute()
        return count

    def remove(self, doc_id):
        pipe = self.conn.pipeline(transaction=False)
        cache = self.keys.for_cache(doc_id)

        # First, we remove the document id from the terms it is related to;
        for term in self.conn.smembers(cache):
            pipe.zrem(self.keys.for_term(term), doc_id)
        pipe.execute()

        # The second step is to remove the cache key, we don't need it anymore;
        self.conn.delete(cache)

        # Last but not least, we need to get rid of the actual document now;
        self.conn.hdel(self.keys.for_docs(), doc_id)

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
    """Magic autocomplete support for your python project

    Suggestive provides autocomplete with some few lines of code:

        >>> import suggestive
        >>> s = suggestive.Suggestive('db', backend=suggestive.DummyBackend())
        >>> s.index([{'id': 0, 'name': 'Lincoln'}], field='name', score='id')
        >>> s.suggest('li')
        [{'id': 0, 'name': 'Lincoln'}]

    ## Backends

    Suggestive currently supports to backends: memory and redis.

    ### Memory backend

    The memory backend uses native python data types and all the data will be
    destroyed when the your instance of the `Suggestive` class gets freed by
    python.

    ### Unit tests of features that use suggestive

    One of the main reasons for supporting a dummy backend that doesn't persist
    data is to make it easier to write unit-tests for code that deppends on
    this library. You don't have to mock anything. Just pass an instance of the
    `DummyBackend` class to the `backend` parameter.

    ### Redis backend

    This is the production backend. Suggestive stores each piece of the indexed
    term in a separate key in redis. Each term key is a sorted set containing
    the id of the document that contains that term.

    The documents are jsonified and saved inside of a hash table. This might
    consume a lot of memory, be careful and keep your documents as small as
    possible!
    """

    def __init__(self, db, backend):
        self.db = db
        self.backend = backend

    def index(self, data_source, field, score='score'):
        self.backend.index(data_source, field, score=score)

    def remove(self, doc_id):
        self.backend.remove(doc_id)

    def suggest(self, term, words=False, limit=-1, offset=0):
        return self.backend.query(
            unidecode(term), words=words, limit=limit, offset=offset)
