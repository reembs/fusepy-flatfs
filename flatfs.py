#!/usr/bin/env python

from __future__ import with_statement
import hashlib
import os
import sys
import errno
import sqlite3
from stat import S_IFDIR
from time import time

from fuse import FUSE, FuseOSError, Operations


def _hash_path(partial):
    return hashlib.sha224(partial).hexdigest()


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root

        db_path = self.root + '/.flatfs_structure.sqlite'

        if not os.path.isfile(db_path):
            files = os.listdir(root)
            if len(files) == 0:
                self.conn = sqlite3.connect(db_path)
                self._create_structure()
            else:
                raise FuseOSError(errno.ENOANO)
        else:
            self.conn = sqlite3.connect(db_path)

    # Helpers
    # =======
    def _create_structure(self):
        c = self.conn.cursor()

        # Create table
        c.execute('''CREATE TABLE handles
                     (hash text, name text, parent_path text, is_dir integer)''')

        hash_path = _hash_path("/")

        # Insert a row for root directory
        c.execute("INSERT INTO handles VALUES (?,?,?,?)", (hash_path, "/", None, 1))

        # Save (commit) the changes
        self.conn.commit()

    def _full_path(self, partial):
        hash_path = _hash_path(partial)

        path = os.path.join(self.root, hash_path)

        return path

    def _is_dir(self, path):
        handle = self._get_handle(path)
        return handle is not None and handle[3]

    def _get_handle(self, path):
        c = self.conn.cursor()

        c.execute('''SELECT * FROM handles
                     WHERE hash=?''', (_hash_path(path),))

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
            res.append(row[2][1:] + row[1])
            row = c.fetchone()

        return res

    def _create_handle(self, path, is_dir):
        c = self.conn.cursor()

        path_split = path[1:].split('/')

        parent = "/" + "/".join(path_split[:-1])
        name = path_split[-1]

        dir_flag = 0
        if is_dir:
            dir_flag = 1

        c.execute("INSERT INTO handles VALUES (?,?,?,?)", (_hash_path(path), name, parent, dir_flag))

        self.conn.commit()

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        if not os.access(self.root, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        if self._is_dir(path):
            return
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        if self._is_dir(path):
            return
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        if self._is_dir(path):
            st = os.lstat(self.root)
        else:
            st = os.lstat(self._full_path(path))

        return dict((key, getattr(st, key)) for key in
                    ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

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
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        handle = self._get_handle(path)
        if handle is not None:
            raise FuseOSError(errno.ENOANO)
        os.mkdir(self._full_path(path), mode)
        self._create_handle(path, True)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        handle = self._get_handle(name)
        if handle is not None:
            raise FuseOSError(errno.ENOANO)
        os.symlink(target, self._full_path(name))
        self._create_handle(name, False)

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        if self._is_dir(path):
            return os.utime(self._full_path(self.root), times)
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        handle = self._get_handle(path)
        if handle is not None:
            raise FuseOSError(errno.ENOANO)
        os.open(self._full_path(path), os.O_WRONLY | os.O_CREAT, mode)
        self._create_handle(path, False)

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
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True, debug=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
