import os
import errno
from fuse import FuseOSError
import hashlib
import pickle

from unqlite import UnQLite

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

    def __del__(self):
        pass

    def get(self, key, default=None):
        if key in self._store:
            return pickle.loads(self._store[key])
        return default

    def put(self, key, val):
        self._store[key] = pickle.dumps(val)

    def remove(self, key):
        res = self.get(key)
        if res is not None:
            del self._store[key]
        return res

    def _create_structure(self, stv):
        _hash_path = hash_path("/")
        # Insert a row for root directory
        self.put(_hash_path, (_hash_path, "/", None, 1, stv, None))
        self.put('l_' + _hash_path, [])

    def _create_connections(self, db_path):
        self._store = UnQLite(db_path)
