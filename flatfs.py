#!/usr/bin/env python

from __future__ import with_statement
import os
from stat import S_IFDIR
import sys
import errno
from time import time
from key_val_store import HandleStore
from key_val_store import hash_path

from fuse import FUSE, FuseOSError, Operations

def _split_path(path):
    path_split = os.path.split(path[1:])
    return path_split[0], path_split[1]

# noinspection PyNoneFunctionAssignment,PyMethodMayBeStatic
class FlatFS(Operations):
    def __init__(self, root, mount_point):
        self.root = root
        self.mount_point = mount_point

        db_path = self.root + '/.store.unqlite'

        st_dict = self._get_st_dict(os.lstat(self.root))
        self.gid = st_dict['st_gid']
        self.uid = st_dict['st_uid']

        stv = self._get_st_dict(os.lstat(self.root))

        self.store = HandleStore(db_path, stv)

        handle = self._get_handle_path('/')
        if handle is None:
            handle = (hash_path('/'), '/', None, 1, stv, None)
            self.store.put(hash_path('/'), handle)

    def init(self, path):
        super(FlatFS, self).init(path)

    def __del__(self):
        del self.store

    # Helpers
    # =======
    def _full_path(self, partial):
        path = self._get_full_path_hash(hash_path(partial))
        return path

    def _full_path_handle(self, handle):
        return os.path.join(self.root, handle[0])

    def _get_full_path_hash(self, _hash_path):
        path = os.path.join(self.root, _hash_path)
        return path

    def _is_dir(self, path):
        handle = self._get_handle_path(path)
        return self._is_dir_handle(handle)

    def _is_dir_handle(self, handle):
        return handle is not None and handle[3]

    def _get_handle_path(self, path):
        return self._get_handle_hash(hash_path(path))

    def _get_handle_hash(self, path_hash):
        return self.store.get(path_hash)

    def _list_dir(self, path):
        return self.store.get("l_" + hash_path(path))

    def _copy_handle(self, handle, key=None, name=None, stv=None):
        if key is None:
            key = handle[0]
        if name is None:
            name = handle[1]
        if stv is None:
            stv = handle[4]
        return key, name, handle[2], handle[3], stv, handle[5]

    def _create_handle(self, path, is_dir, dir_stv=None, link_path=None):
        dir_flag = 0
        if is_dir:
            dir_flag = 1

        parent, name = _split_path(path)

        handle = (hash_path(path), name, parent, dir_flag, dir_stv, link_path)

        self.store.put(handle[0], handle)
        if is_dir:
            self.store.put("l_" + handle[0], [])

        parent_dir_key = "l_" + hash_path("/" + handle[2])
        _dir = self.store.get(parent_dir_key)
        _dir.append(handle[1])

        self.store.put(parent_dir_key, _dir)

        return handle

    def _rename_handle(self, old, new):
        parent, name = _split_path(new)

        self._remove_handle(new)

        parent, old_name = os.path.split(old)

        parent_hash = hash_path(parent)
        parent_l = self.store.get("l_" + parent_hash)
        parent_l.remove(old_name)
        parent_l.append(name)
        self.store.put("l_" + parent_hash, parent_l)

        handle = self._remove_handle(old)
        handle = self._copy_handle(handle, key=hash_path(new), name=name)

        self.store.put(handle[0], handle)

    def _update_dir_stv(self, handle, new_stv):
        handle = self._copy_handle(handle, stv=new_stv)
        self.store.put(handle[0], handle)

    def _remove_handle(self, path):
        _hash_path = hash_path(path)

        handle = self.store.remove(_hash_path)
        if handle is not None:
            parent, name = os.path.split(path)

            if handle[3] == 1:
                self.store.remove('l_' + _hash_path)

            parent_hash = hash_path(parent)
            parent_l = self.store.get("l_" + parent_hash)
            if name in parent_l:
                parent_l.remove(name)
                self.store.put("l_" + parent_hash, parent_l)

        return handle

    def _get_dir_stv(self, handle):
        return handle[4]

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
            res = os.symlink(hash_path(fuse_path), self._full_path(name))
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
