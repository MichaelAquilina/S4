# -*- coding: utf-8 -*-

import gzip
import json
import logging
import os
import shutil
import tempfile
from os import scandir

import filelock
import magic
import pathspec

from s4.clients import SyncClient, SyncObject

logger = logging.getLogger(__name__)


def get_local_client(target):
    return LocalSyncClient(target)


def traverse(path, ignore_files=None):
    if not os.path.exists(path):
        return
    if ignore_files is None:
        ignore_files = []

    for item in scandir(path):
        full_path = os.path.join(path, item.name)
        spec = pathspec.PathSpec.from_lines(
            pathspec.patterns.GitWildMatchPattern, ignore_files
        )
        if spec.match_file(full_path):
            logger.debug("Ignoring %s", item)
            continue

        if item.is_dir():
            for result in traverse(item.path, ignore_files):
                yield os.path.join(item.name, result)
        else:
            yield item.name


class LocalSyncClient(SyncClient):
    DEFAULT_IGNORE_FILES = [".index", ".s4lock"]
    LOCK_FILE_NAME = ".s4lock"

    def __init__(self, path):
        self.path = path
        self.reload_index()
        self.reload_ignore_files()
        self._lock = filelock.FileLock(self.lock_file)

    @property
    def lock_file(self):
        return self.get_uri(self.LOCK_FILE_NAME)

    def ensure_path(self, path):
        parent = os.path.dirname(path)
        if not os.path.exists(parent):
            os.makedirs(parent)

    def lock(self, timeout=10):
        """
        Advisory lock.
        Use to ensure that only one LocalSyncClient is working on the Target at the same time.
        """
        logger.debug("Locking %s", self.lock_file)
        if not os.path.exists(self.lock_file):
            self.ensure_path(self.lock_file)
            with open(self.lock_file, "w"):
                os.utime(self.lock_file)
        self._lock.acquire(timeout=timeout)

    def unlock(self):
        """
        Unlock the active advisory lock.
        """
        logger.debug("Releasing lock %s", self.lock_file)
        self._lock.release()
        try:
            os.unlink(self.lock_file)
        except FileNotFoundError:
            pass

    def get_client_name(self):
        return "local"

    def __repr__(self):
        return "LocalSyncClient<{}>".format(self.path)

    def get_uri(self, key=""):
        return os.path.join(self.path, key)

    def index_path(self):
        return os.path.join(self.path, ".index")

    def put(self, key, sync_object, callback=None):
        path = os.path.join(self.path, key)
        self.ensure_path(path)

        BUFFER_SIZE = 4096
        fd, temp_path = tempfile.mkstemp()

        try:
            with open(temp_path, "wb") as fp_1:
                while True:
                    data = sync_object.fp.read(BUFFER_SIZE)
                    fp_1.write(data)
                    if callback is not None:
                        callback(len(data))
                    if len(data) < BUFFER_SIZE:
                        break
            shutil.move(temp_path, path)
        except Exception:
            os.remove(temp_path)
            raise
        finally:
            os.close(fd)

        self.set_remote_timestamp(key, sync_object.timestamp)

    def get(self, key):
        path = os.path.join(self.path, key)
        if os.path.exists(path):
            fp = open(path, "rb")
            stat = os.stat(path)
            return SyncObject(fp, stat.st_size, stat.st_mtime)
        else:
            return None

    def delete(self, key):
        path = os.path.join(self.path, key)
        if os.path.exists(path):
            os.remove(path)
            return True
        else:
            return False

    def reload_index(self):
        self.index = self._load_index()

    def _load_index(self):
        index_path = self.index_path()
        if not os.path.exists(index_path):
            return {}

        content_type = magic.from_file(index_path, mime=True)
        if content_type in ("application/json", "text/plain"):
            logger.debug("Detected %s encoding for reading index", content_type)
            method = open
        elif content_type in ("application/gzip", "application/x-gzip"):
            logger.debug("Detected gzip encoding for reading index")
            method = gzip.open
        else:
            raise ValueError("Index is of unknown type", content_type)

        with method(index_path, "rt") as fp:
            data = json.load(fp)
        return data

    def flush_index(self, compressed=True):
        if compressed:
            logger.debug("Using gzip encoding for writing index")
            method = gzip.open
        else:
            logger.debug("Using plaintext encoding for writing index")
            method = open

        fd, temp_path = tempfile.mkstemp()
        with method(temp_path, "wt") as fp:
            json.dump(self.index, fp)

        os.close(fd)

        shutil.move(temp_path, self.index_path())

    def get_local_keys(self):
        return list(traverse(self.path, ignore_files=self.ignore_files))

    def get_real_local_timestamp(self, key):
        full_path = os.path.join(self.path, key)
        if os.path.exists(full_path):
            return os.path.getmtime(full_path)
        else:
            return None

    def get_index_keys(self):
        return self.index.keys()

    def get_index_local_timestamp(self, key):
        return self.index.get(key, {}).get("local_timestamp")

    def get_all_real_local_timestamps(self):
        result = {}
        for key in self.get_local_keys():
            result[key] = self.get_real_local_timestamp(key)
        return result

    def get_all_remote_timestamps(self):
        return {key: value.get("remote_timestamp") for key, value in self.index.items()}

    def get_all_index_local_timestamps(self):
        return {key: value.get("local_timestamp") for key, value in self.index.items()}

    def set_index_local_timestamp(self, key, timestamp):
        if key not in self.index:
            self.index[key] = {}
        self.index[key]["local_timestamp"] = timestamp

    def get_size(self, key):
        path = self.get_uri(key)
        if os.path.exists(path):
            return os.stat(path).st_size
        else:
            return 0

    def get_remote_timestamp(self, key):
        return self.index.get(key, {}).get("remote_timestamp")

    def set_remote_timestamp(self, key, timestamp):
        if key not in self.index:
            self.index[key] = {}
        self.index[key]["remote_timestamp"] = timestamp

    def reload_ignore_files(self):
        ignore_path = os.path.join(self.path, ".syncignore")

        if os.path.exists(ignore_path):
            with open(ignore_path, "r") as fp:
                ignore_list = fp.read().split("\n")
        else:
            ignore_list = []

        self.ignore_files = self.DEFAULT_IGNORE_FILES + ignore_list
