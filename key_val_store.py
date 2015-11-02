import os
import errno
from fuse import FuseOSError
import hashlib
import pickle
import pylru

from bsddb3 import db

CACHE_SIZE = 50000

def hash_path(partial):
    return hashlib.sha224(partial.encode('utf-8')).hexdigest()

class HandleStore:
    def __init__(self, path, stv):
        self._path = path
        self.root = os.path.split(path)[0]
        if not os.path.isfile(path):
            files = os.listdir(self.root)
            if len(files) == 0:
                self._create_connections(path)
                self._create_structure(stv)
            else:
                raise FuseOSError(errno.ENOANO)
        else:
            self._create_connections(path)

        self._pre_populate_cache()

    def __del__(self):
        self._store.close()

    def get(self, key, default=None):
        if key in self.cache:
            return self.cache[key]

        value = self._store.get(key, None)
        if value is not None:
            value = pickle.loads(value)
            self.cache[key] = value
            return value

        return default

    def put(self, key, val):
        self._store.put(key, pickle.dumps(val))
        self.cache[key] = val

    def remove(self, key):
        if key in self.cache:
            del self.cache[key]

        res = self.get(key)
        if res is not None:
            self._store.delete(key)

        return res

    def _create_structure(self, stv):
        _hash_path = hash_path("/")
        # Insert a row for root directory
        self.put(_hash_path, (_hash_path, "/", None, 1, stv, None))
        self.put('l_' + _hash_path, [])

    def _create_connections(self, db_path):
        self._store = db.DB()
        self._store.open(db_path, None, db.DB_HASH, db.DB_CREATE)
        self.cache = pylru.lrucache(CACHE_SIZE)

    def _pre_populate_cache(self):
        i = 0
        cursor = self._store.cursor()

        n = cursor.next()
        while n is not None:
            self.cache[n[0]] = pickle.loads(n[1])
            n = cursor.next()
            i += 1
            if i >= CACHE_SIZE:
                break

        cursor.close()
