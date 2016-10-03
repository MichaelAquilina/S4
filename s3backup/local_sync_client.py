# -*- coding: utf-8 -*-

import hashlib
import json
import logging
import os

logger = logging.getLogger(__name__)


IGNORED_FILES = {'.syncindex.json.gz', '.syncindex'}
BUFFER = 4096


def create_parent_directories(path):
    path = '/'.join(path.split('/')[:-1])
    if not os.path.exists(path):
        os.makedirs(path)


def traverse(path):
    for item in os.listdir(path):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            for result in traverse(full_path):
                yield os.path.join(item, result)
        elif item not in IGNORED_FILES:
            yield item


def generate_index(target_dir):
    result = {}
    for key in traverse(target_dir):
        object_path = os.path.join(target_dir, key)
        md5 = hashlib.md5()
        with open(object_path, 'rb') as fp:
            while True:
                data = fp.read(BUFFER)
                md5.update(data)
                if len(data) < BUFFER:
                    break

        stat = os.stat(object_path)
        result[key] = {
            'timestamp': None,
            'LastModified': stat.st_mtime,
            'size': stat.st_size,
            'md5': md5.hexdigest(),
        }
    return result


class LocalSyncClient(object):
    SYNC_INDEX = '.syncindex'

    def __init__(self, local_dir):
        self.local_dir = local_dir
        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)

        # TODO: optional things like md5 should be only required if necessary
        self.prev_index = self._get_prev_index()
        self.curr_index = generate_index(self.local_dir)

        for key, metadata in self.prev_index.items():
            if key not in self.curr_index:
                # It has been deleted
                pass
            else:
                self.curr_index[key]['timestamp'] = metadata['timestamp']

    def _get_prev_index(self):
        if os.path.exists(self.sync_index_path):
            with open(self.sync_index_path, 'r') as fp:
                return json.load(fp)
        else:
            return {}

    @property
    def sync_index_path(self):
        return os.path.join(self.local_dir, self.SYNC_INDEX)

    def get_object_timestamp(self, key):
        return self.curr_index.get(key, {}).get('timestamp')

    def get_object_md5(self, key):
        object_path = os.path.join(self.local_dir, key)
        with open(object_path, 'rb') as fp:
            md5 = hashlib.md5()
            md5.update(fp.read())
        return md5.hexdigest()

    def update_sync_index(self):
        with open(self.sync_index_path, 'w') as fp:
            json.dump(self.curr_index, fp)

    def keys(self):
        return self.curr_index.keys()

    def put_object(self, key, fp, timestamp, callback=None):
        object_path = os.path.join(self.local_dir, key)

        if not os.path.exists(object_path):
            create_parent_directories(object_path)

        try:
            md5 = hashlib.md5()
            with open(object_path, 'wb') as fp2:
                while True:
                    data = fp.read(BUFFER)
                    md5.update(data)
                    fp2.write(data)
                    if callback is not None:
                        callback(len(data))
                    if len(data) < BUFFER:
                        break

            object_stat = os.stat(object_path)
            self.curr_index[key] = {
                'timestamp': timestamp,
                'md5': md5.hexdigest(),
                'LastModified': object_stat.st_mtime,
                'size': object_stat.st_size,
            }
        except:
            # If something goes wrong, treat it as a transaction and delete
            # the file so that the index is not curropted where possible
            os.remove(object_path)
            raise

    def get_object(self, key):
        # TODO: Update this to use the current index
        object_path = os.path.join(self.local_dir, key)
        stat = os.stat(object_path)
        return (
            stat.st_size,
            open(object_path, 'rb'),
            stat,
        )
