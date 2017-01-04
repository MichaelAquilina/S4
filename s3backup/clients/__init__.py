# -*- coding: utf-8 -*-

import datetime


class SyncAction(object):
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    CONFLICT = 'CONFLICT'
    NONE = 'NONE'

    def __init__(self, action, timestamp):
        self.action = action
        self.timestamp = timestamp

    def __eq__(self, other):
        if not isinstance(other, SyncAction):
            return False
        return self.action == other.action and self.timestamp == other.timestamp

    def __repr__(self):
        if self.timestamp is not None:
            timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        else:
            timestamp = None
        return 'SyncAction<{}, {}>'.format(self.action, timestamp)


class SyncObject(object):
    def __init__(self, fp, timestamp):
        self.fp = fp
        self.timestamp = timestamp

    def __repr__(self):
        return 'SyncObject<{}, {}>'.format(self.fp, self.timestamp)
