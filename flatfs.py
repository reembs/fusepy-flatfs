#!/usr/bin/env python

from __future__ import with_statement
import hashlib
import os
import pickle
from stat import S_IFDIR
import sys
import errno
import sqlite3
from time import time

from fuse import FUSE, FuseOSError, Operations


def _hash_path(partial):
    return hashlib.sha224(partial.encode('utf-8')).hexdigest()


def _split_path(path):
    path_split = path[1:].split('/')
    parent = "/" + "/".join(path_split[:-1])
    name = path_split[-1]
    return name, parent


# noinspection PyNoneFunctionAssignment,PyMethodMayBeStatic
class FlatFS(Operations):
    def __init__(self, root, mount_point):
        self.root = root
        self.mount_point = mount_point

        db_path = self.root + '/.flatfs_structure.sqlite'

        st_dict = self._get_st_dict(os.lstat(self.root))
        self.gid = st_dict['st_gid']
        self.uid = st_dict['st_uid']

        if not os.path.isfile(db_path):
            files = os.listdir(root)
            if len(files) == 0:
                self.conn = sqlite3.connect(db_path)
                self._create_structure()
            else:
                raise FuseOSError(errno.ENOANO)
        else:
            self.conn = sqlite3.connect(db_path)

    def __del__(self):
        self._vacuum_db()

    # Helpers
    # =======
    def _create_structure(self):
        c = self.conn.cursor()

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

        hash_path = _hash_path("/")

        stv = self._get_st_dict(os.lstat(self.root))

        # Insert a row for root directory
        c.execute("INSERT INTO handles VALUES (?,?,?,?,?,?)", (hash_path, "/", None, 1, pickle.dumps(stv), None))

        # Save (commit) the changes
        self.conn.commit()

    def _full_path(self, partial):
        hash_path = _hash_path(partial)
        path = self._get_full_path_hash(hash_path)
        return path

    def _full_path_handle(self, handle):
        return os.path.join(self.root, handle[0])

    def _get_full_path_hash(self, hash_path):
        path = os.path.join(self.root, hash_path)
        return path

    def _is_dir(self, path):
        handle = self._get_handle_path(path)
        return self._is_dir_handle(handle)

    def _is_dir_handle(self, handle):
        return handle is not None and handle[3]

    def _get_handle_path(self, path):
        return self._get_handle_hash(_hash_path(path))

    def _get_handle_hash(self, path_hash):
        c = self.conn.cursor()

        c.execute('''SELECT * FROM handles
                     WHERE hash=?''', (path_hash,))

        row = c.fetchone()
        if row is not None:
            return row

        return None

    def _list_dir(self, path):
        c = self.conn.cursor()

        # Create table
        c.execute('''SELECT * FROM handles
                     WHERE parent_path=?''', (path,))

        res = []

        row = c.fetchone()
        while row is not None:
            res.append(row[1])
            row = c.fetchone()

        return res

    def _create_handle(self, path, is_dir, dir_stv=None, link_path=None):
        c = self.conn.cursor()

        name, parent = _split_path(path)

        dir_flag = 0
        if is_dir:
            dir_flag = 1

        encoded_dir_stv = None
        if dir_stv is not None:
            encoded_dir_stv = pickle.dumps(dir_stv)

        c.execute("INSERT INTO handles VALUES (?,?,?,?,?,?)",
                  (_hash_path(path), name, parent, dir_flag, encoded_dir_stv, link_path))

        self.conn.commit()

    def _rename_handle(self, old, new):
        name, parent = _split_path(new)

        self._remove_handle(new)

        c = self.conn.cursor()
        c.execute('UPDATE handles SET hash=?, name=? WHERE hash=?', (_hash_path(new), name, _hash_path(old)))
        c.fetchone()

        self.conn.commit()

    def _update_dir_stv(self, handle, new_stv):
        c = self.conn.cursor()

        new_stv_encoded = pickle.dumps(new_stv)

        c.execute('UPDATE handles SET dir_stv=? WHERE hash=?', (new_stv_encoded, handle[0]))
        c.fetchone()
        self.conn.commit()

    def _remove_handle(self, path):
        c = self.conn.cursor()
        c.execute('DELETE FROM handles WHERE hash=?', (_hash_path(path),))
        c.fetchone()
        self.conn.commit()

    def _vacuum_db(self):
        c = self.conn.cursor()
        c.execute('VACUUM;')
        c.fetchone()
        self.conn.commit()

    def _get_dir_stv(self, handle):
        return pickle.loads(handle[4])

    def _create_new_dir_st(self, mode):
        return dict(st_mode=(S_IFDIR | mode), st_nlink=2, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(),
                    st_gid=self.gid, st_uid=self.uid)

    def _get_st_dict(self, st):
        return dict((key, getattr(st, key)) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        if not os.access(self.root, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        handle = self._get_handle_path(path)
        if self._is_dir_handle(handle):
            stv = self._get_dir_stv(handle)
            stv['st_mode'] &= 0770000
            stv['st_mode'] |= mode
            self._update_dir_stv(handle, stv)
            return 0
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        handle = self._get_handle_path(path)
        if self._is_dir_handle(handle):
            stv = self._get_dir_stv(handle)
            stv['st_uid'] = uid
            stv['st_gid'] = gid
            self._update_dir_stv(handle, stv)
            return 0
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        handle = self._get_handle_path(path)
        if self._is_dir_handle(handle):
            return self._get_dir_stv(handle)
        else:
            st = os.lstat(self._full_path(path))
            return self._get_st_dict(st)

    def readdir(self, path, fh):
        dirents = ['.', '..']
        dirents.extend(self._list_dir(path))
        for r in dirents:
            yield r

    def readlink(self, path):
        return os.readlink(self._full_path(path))

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        list_dir = self._list_dir(path)
        if len(list_dir) > 0:
            raise FuseOSError(errno.ENOANO)
        self._remove_handle(path)

    def mkdir(self, path, mode):
        handle = self._get_handle_path(path)
        if handle is not None:
            raise FuseOSError(errno.ENOANO)
        self._create_handle(path, True, dir_stv=self._create_new_dir_st(mode))
        return

    def statfs(self, path):
        handle = self._get_handle_path(path)
        if self._is_dir_handle(handle):
            stv = os.statvfs(self.root)
        else:
            full_path = self._full_path(path)
            stv = os.statvfs(full_path)

        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        res = os.unlink(self._full_path(path))
        self._remove_handle(path)
        return res

    def symlink(self, name, target):
        handle = self._get_handle_path(name)
        if handle is not None:
            raise FuseOSError(errno.ENOANO)

        path = os.path.abspath(os.path.join(self.mount_point, "." + os.path.split(name)[0], target))

        if path.startswith(self.mount_point):
            fuse_path = path[len(self.mount_point):]
            res = os.symlink(_hash_path(fuse_path), self._full_path(name))
        else:
            res = os.symlink(path, self._full_path(name))

        self._create_handle(name, False, link_path=target)

        return res

    def rename(self, old, new):
        handle = self._get_handle_path(old)
        if handle is None:
            raise FuseOSError(errno.ENOANO)
        res = os.rename(self._full_path(old), self._full_path(new))
        self._rename_handle(old, new)
        return res

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        handle = self._get_handle_path(path)
        if self._is_dir_handle(handle):
            stv = self._get_dir_stv(handle)
            atime, mtime = times if times else (time(), time())
            stv['st_atime'] = atime
            stv['st_mtime'] = mtime
            self._update_dir_stv(handle, stv)
            return
        elif handle[5] is not None:
            return

        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        handle = self._get_handle_path(path)
        if handle is not None:
            raise FuseOSError(errno.ENOANO)
        res = os.open(self._full_path(path), os.O_WRONLY | os.O_CREAT, mode)
        self._create_handle(path, False)
        return res

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(FlatFS(root, mountpoint), mountpoint, nothreads=True, foreground=True, debug=False)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
