# Suggestive

Magically add auto-complete to your python project.

Suggestive is a very simple python library that allows you to easily add the
auto-complete feature to your project. It works for any kind of python project,
even if it's not web based.

## Talk is cheap show me the code

Well, usually you have follow two steps to get your auto-complete running
properly. First, you need to index things, then you can search for them. Let's
see examples for both steps separately.

### Indexing
```python
>>> data = [
...     {"id": 0, "name": "Lincoln", "score": 123},
...     {"id": 1, "name": "Livia", "score": 12345},
...     {"id": 5, "name": "Linus", "score": 123456},  # Rita's brother! :)
... ]
>>> import redis
>>> from suggestive import Suggestive, RedisBackend
>>> s = Suggestive(RedisBackend(conn=redis.StrictRedis()))
>>> s.index(data, field='name')
```

That's it. Your data is cached inside of `redis`.

### Querying

Things are fairly easier in this step. You just need to setup the suggestive
instance and use the `suggest` method:

```python
>>> import redis
>>> from suggestive import Suggestive, RedisBackend
>>> s = Suggestive(RedisBackend(conn=redis.StrictRedis()))
>>> s.suggest('lin')
[{u'score': 123456, u'id': 5, u'name': u'Linus'}, {u'score': 123, u'id': 0, u'name': u'Lincoln'}]
```

### Outro

Our API is not pretty stable yet, the way we choose the pass the backend
instance to the `Suggestive` instance will be changed to use a `uri`. But
we will definitely keep the compatibility with this version, since it
makes things way easier when writing tests.

There are some very simple things that we're not doing either, like adding
limit and offset parameters to the `suggest()` method. We're planning to
add this feature and some others as soon as the rest of the code gets more
stable.
