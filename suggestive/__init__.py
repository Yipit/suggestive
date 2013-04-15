class DummyBackend(object):
    def __init__(self):
        self.data = {}

    def index(self, data_source):
        self.data = data_source

    def query(self, term, field, sort):
        result = filter(lambda x: x[field].startswith(term), self.data)
        if sort:
            result.sort(key=lambda x: x[sort])
        return result


class Suggestive(object):

    def __init__(self, db, backend):
        self.db = db
        self.backend = backend

    def index(self, data_source, sort_key=None):
        self.backend.index(data_source)

    def suggest(self, term, field, sort=None):
        return self.backend.query(term, field, sort)
