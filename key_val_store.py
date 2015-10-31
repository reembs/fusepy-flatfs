import os
import errno
from fuse import FuseOSError
import sqlite3
import hashlib

def hash_path(partial):
    return hashlib.sha224(partial.encode('utf-8')).hexdigest()

class HandleStore:
    def __init__(self, path, stv):
        self._path = path
        self._store = {}

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

        self._flush_disk_to_mem()

    def __del__(self):
        self._flush_mem_to_disk()
        self._vacuum_db()

    def _vacuum_db(self):
        c = self.disk_conn.cursor()
        c.execute('VACUUM;')
        c.fetchone()
        self.disk_conn.commit()

    def get(self, key):
        return self._store.get(key, None)

    def add(self, key, val):
        self._store[key] = val

    def remove(self, key):
        res = None
        if key in self._store:
            res = self._store[key]
            del self._store[key]
        return res

    def _create_structure(self, stv):
        c = self.disk_conn.cursor()

        # Create table
        c.execute('''CREATE TABLE handles (
                     hash text NOT NULL UNIQUE,
                     name text NOT NULL,
                     parent_path text,
                     is_dir integer NOT NULL,
                     dir_stv text,
                     link_path text);''')

        c.execute('''CREATE INDEX index_hash ON handles (hash);''')
        c.execute('''CREATE INDEX index_parent_path ON handles (parent_path);''')

        _hash_path = hash_path("/")

        # Insert a row for root directory
        c.execute("INSERT INTO handles VALUES (?,?,?,?,?,?)", (_hash_path, "/", None, 1, stv, None))

        # Save (commit) the changes
        self.disk_conn.commit()

    def _flush_disk_to_mem(self):
        dc = self.disk_conn.cursor()

        dc.execute("SELECT * FROM handles")

        row = dc.fetchone()
        while row is not None:
            try:
                self._store[row[0]] = row

                if row[2] is None:
                    dir_key = 'l_' + hash_path("/")
                else:
                    if row[3] == 0:
                        dir_key = 'l_' + hash_path("/" + row[2])
                    else:
                        dir_key = 'l_' + row[0]

                parent_l = self._store.get(dir_key, [])

                if row[3] == 0:
                    parent_l.append(row[1])

                self._store[dir_key] = parent_l
            finally:
                row = dc.fetchone()

    def _flush_mem_to_disk(self):
        dc = self.disk_conn.cursor()

        dc.execute("DELETE FROM handles")
        dc.fetchone()

        for key in self._store.keys():
            if key.startswith('l_') is False:
                dc.execute("INSERT INTO handles VALUES (?,?,?,?,?,?)", self._store[key])
                dc.fetchone()

        self.disk_conn.commit()

    def _create_connections(self, db_path):
        self.disk_conn = sqlite3.connect(db_path)
