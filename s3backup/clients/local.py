# -*- coding: utf-8 -*-

import json
import os


def traverse(path):
    IGNORED_FILES = {'.index'}
    for item in os.listdir(path):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            for result in traverse(full_path):
                yield os.path.join(item, result)
        elif item not in IGNORED_FILES:
            yield item


class LocalSyncClient(object):
    def __init__(self, path):
        self.path = path

    def index_path(self):
        return os.path.join(self.path, '.index')

    def put(self, key, fp0):
        with open(os.path.join(self.path, key), 'wb') as fp1:
            fp1.write(fp0.read())

    def get(self, key):
        return open(os.path.join(self.path, key), 'rb')

    def delete(self, key):
        os.remove(os.path.join(self.path, key))

    def get_index_state(self):
        if not os.path.exists(self.index_path()):
            return {}

        with open(self.index_path(), 'r') as fp:
            data = json.load(fp)

        return data

    def get_current_state(self):
        results = {}
        for relative_path in traverse(self.path):
            full_path = os.path.join(self.path, relative_path)
            stat = os.stat(full_path)

            results[relative_path] = dict(timestamp=stat.st_mtime)
        return results

    def update_index(self):
        data = self.get_current_state()
        with open(self.index_path(), 'w') as fp:
            json.dump(data, fp)
