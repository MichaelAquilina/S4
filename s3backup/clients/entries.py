# -*- coding: utf-8 -*-


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
