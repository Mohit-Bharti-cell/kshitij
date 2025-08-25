class _DummyDB:
    def init_app(self, *a, **k):
        pass

db = _DummyDB()
