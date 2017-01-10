# -*- coding: utf-8 -*-

import fnmatch
import json
import logging
import os

from s3backup.clients import SyncClient, SyncObject

logger = logging.getLogger('s3backup')


def traverse(path, ignore_files=None):
    if ignore_files is None:
        ignore_files = []

    for item in sorted(os.listdir(path)):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            for result in traverse(full_path, ignore_files):
                yield os.path.join(item, result)
        elif not any(fnmatch.fnmatch(item, pattern) for pattern in ignore_files):
            yield item
        else:
            logger.debug('Ingoring %s', item)


class LocalSyncClient(SyncClient):
    def __init__(self, path, ignore_files=None):
        self.path = path
        self.index = self.load_index()
        self.ignore_files = ['.index']

        if ignore_files is not None:
            self.ignore_files.extend(ignore_files)

    def __repr__(self):
        return 'LocalSyncClient<{}>'.format(self.path)

    def get_uri(self):
        return self.path

    def index_path(self):
        return os.path.join(self.path, '.index')

    def put(self, key, sync_object, callback=None):
        path = os.path.join(self.path, key)

        parent = os.path.dirname(path)
        if not os.path.exists(parent):
            os.makedirs(parent)

        BUFFER_SIZE = 4096

        with open(path, 'wb') as fp1:
            while True:
                data = sync_object.fp.read(BUFFER_SIZE)
                fp1.write(data)
                if callback is not None:
                    callback(len(data))
                if len(data) < BUFFER_SIZE:
                    break

        self.set_remote_timestamp(key, sync_object.timestamp)

    def get(self, key):
        path = os.path.join(self.path, key)
        if os.path.exists(path):
            fp = open(path, 'rb')
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

    def load_index(self):
        if not os.path.exists(self.index_path()):
            return {}

        with open(self.index_path(), 'r') as fp:
            data = json.load(fp)
        return data

    def flush_index(self):
        with open(self.index_path(), 'w') as fp:
            json.dump(self.index, fp)

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
        return self.index.get(key, {}).get('local_timestamp')

    def set_index_local_timestamp(self, key, timestamp):
        if key not in self.index:
            self.index[key] = {}
        self.index[key]['local_timestamp'] = timestamp

    def get_remote_timestamp(self, key):
        return self.index.get(key, {}).get('remote_timestamp')

    def set_remote_timestamp(self, key, timestamp):
        if key not in self.index:
            self.index[key] = {}
        self.index[key]['remote_timestamp'] = timestamp
