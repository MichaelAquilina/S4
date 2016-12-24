# -*- coding: utf-8 -*-

import json
import os


def traverse(path, ignore_files=None):
    if ignore_files is None:
        ignore_files = set()

    for item in sorted(os.listdir(path)):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            for result in traverse(full_path, ignore_files):
                yield os.path.join(item, result)
        elif item not in ignore_files:
            yield item


class LocalSyncClient(object):
    def __init__(self, path):
        self.path = path
        self.index = self.get_index_state()

    def index_path(self):
        return os.path.join(self.path, '.index')

    def put(self, key, fp, remote_timestamp):
        path = os.path.join(self.path, key)
        with open(path, 'wb') as fp1:
            fp1.write(fp.read())

        if key not in self.index:
            self.index[key] = {}
        self.index[key]['remote_timestamp'] = remote_timestamp

    def get(self, key):
        path = os.path.join(self.path, key)
        if os.path.exists(path):
            return open(path, 'rb')

    def delete(self, key):
        # TODO: Figure out a way to distinguish between success and not found
        path = os.path.join(self.path, key)
        if os.path.exists(path):
            os.remove(path)

    def get_index_state(self):
        if not os.path.exists(self.index_path()):
            return {}

        with open(self.index_path(), 'r') as fp:
            data = json.load(fp)

        return data

    def get_current_state(self):
        results = {}
        for relative_path in traverse(self.path, ignore_files={'.index'}):
            full_path = os.path.join(self.path, relative_path)
            stat = os.stat(full_path)

            results[relative_path] = {'local_timestamp': stat.st_mtime}
        return results

    def update_index(self):
        results = self.get_current_state()
        for key in results:
            if key in self.index:
                results[key]['remote_timestamp'] = self.index[key]['remote_timestamp']

        with open(self.index_path(), 'w') as fp:
            json.dump(results, fp)
        self.index = results
