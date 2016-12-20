# -*- coding: utf-8 -*-

import enum
import functools


@functools.total_ordering
class SyncAction(enum.Enum):
    CREATE = 'CREATE'
    DELETE = 'DELETE'
    UPDATE = 'UPDATE'
    CONFLICT = 'CONFLICT'

    def __lt__(self, other):
        return self.value < other.value


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
