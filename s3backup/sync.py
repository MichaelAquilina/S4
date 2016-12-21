# -*- coding: utf-8 -*-

import enum
import functools


@functools.total_ordering
class IndexAction(enum.Enum):
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


def compare_states(current, previous):
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
                yield key, IndexAction.UPDATE
            elif previous_timestamp > current_timestamp:
                yield key, IndexAction.CONFLICT
        elif in_current and not in_previous:
            yield key, IndexAction.CREATE
        elif in_previous and not in_current:
            yield key, IndexAction.DELETE
        else:
            raise ValueError('Reached Unknown state')
