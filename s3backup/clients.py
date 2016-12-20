# -*- coding: utf-8 -*-

import enum
import functools
import json
import os


@functools.total_ordering
class SyncAction(enum.Enum):
    CREATE = 'CREATE'
    DELETE = 'DELETE'
    UPDATE = 'UPDATE'
    CONFLICT = 'CONFLICT'

    def __lt__(self, other):
        return self.value < other.value


def traverse(path):
    IGNORED_FILES = {'.index'}
    for item in os.listdir(path):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            for result in traverse(full_path):
                yield os.path.join(item, result)
        elif item not in IGNORED_FILES:
            yield item


class FileEntry(object):
    def __init__(self, path, timestamp):
        self.path = path
        self.timestamp = timestamp

    def __eq__(self, other):
        if isinstance(other, FileEntry):
            return self.path == other.path and self.timestamp == other.timestamp
        return False

    def __repr__(self):
        return 'FileEntry({}, {})'.format(self.path, self.timestamp)


class LocalSyncClient(object):
    def __init__(self, path):
        self.path = path

    def index_path(self):
        return os.path.join(self.path, '.index')

    def get_index_state(self):
        if not os.path.exists(self.index_path()):
            return {}

        with open(self.index_path(), 'r') as fp:
            data = json.load(fp)

        results = {}
        for path, metadata in data.items():
            results[path] = FileEntry(path, timestamp=metadata['timestamp'])

        return results

    def get_current_state(self):
        results = {}
        for relative_path in traverse(self.path):
            full_path = os.path.join(self.path, relative_path)
            stat = os.stat(full_path)

            results[relative_path] = FileEntry(relative_path, timestamp=stat.st_mtime)
        return results

    def update_index(self):
        data = {}
        for key, entry in self.get_current_state().items():
            data[key] = {'timestamp': entry.timestamp}

        with open(self.index_path(), 'w') as fp:
            json.dump(data, fp)


def compare(current, previous):
    all_keys = set(previous.keys()) | set(current.keys())
    for key in all_keys:
        in_previous = key in previous
        in_current = key in current
        if in_previous and in_current:
            previous_timestamp = previous[key].timestamp
            current_timestamp = current[key].timestamp
            if previous_timestamp == current_timestamp:
                continue
            elif previous_timestamp < current_timestamp:
                yield SyncAction.UPDATE, key
            elif previous_timestamp > current_timestamp:
                yield SyncAction.CONFLICT, key
        elif in_current and not in_previous:
            yield SyncAction.CREATE, key
        elif in_previous and not in_current:
            yield SyncAction.DELETE, key
        else:
            raise ValueError('Reached Unknown state')
