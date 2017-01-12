# -*- coding: utf-8 -*-

import datetime


class SyncState(object):
    UPDATED = 'UPDATED'
    DELETED = 'DELETED'
    CONFLICT = 'CONFLICT'
    NOCHANGES = 'NOCHANGES'
    DOESNOTEXIST = 'DOESNOTEXIST'

    def __init__(self, action, timestamp):
        self.action = action
        self.timestamp = timestamp

    def __eq__(self, other):
        if not isinstance(other, SyncState):
            return False
        return self.action == other.action and self.timestamp == other.timestamp

    def __repr__(self):
        if self.timestamp is not None:
            timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        else:
            timestamp = None
        return 'SyncState<{}, {}>'.format(self.action, timestamp)


class SyncObject(object):
    def __init__(self, fp, total_size, timestamp):
        self.fp = fp
        self.total_size = total_size
        self.timestamp = timestamp

    def __repr__(self):
        return 'SyncObject<{}, {}, {}>'.format(self.fp, self.total_size, self.timestamp)


class SyncClient(object):
    def get_uri(self):
        raise NotImplemented

    def put(self, key, sync_object):
        raise NotImplemented

    def get(self, key):
        raise NotImplemented

    def delete(self, key):
        raise NotImplemented

    def get_local_keys(self):
        raise NotImplemented

    def get_real_local_timestamp(self, key):
        raise NotImplemented

    def get_index_keys(self):
        raise NotImplemented

    def get_index_local_timestamp(self, key):
        raise NotImplemented

    def set_index_local_timestamp(self, key, timestamp):
        raise NotImplemented

    def get_remote_timestamp(self, key):
        raise NotImplemented

    def set_remote_timestamp(self, key, timestamp):
        raise NotImplemented

    def get_all_keys(self):
        local_keys = self.get_local_keys()
        index_keys = self.get_index_keys()
        return list(set(local_keys) | set(index_keys))

    def update_index(self):
        keys = self.get_all_keys()
        index = {}

        for key in keys:
            index[key] = {
                'remote_timestamp': self.get_remote_timestamp(key),
                'local_timestamp': self.get_real_local_timestamp(key),
            }
        self.index = index

    def update_index_entry(self, key):
        self.index[key] = {
            'remote_timestamp': self.get_remote_timestamp(key),
            'local_timestamp': self.get_real_local_timestamp(key),
        }

    def flush_index(self):
        raise NotImplemented

    def get_action(self, key):
        """
        returns the action to perform on this key based on its
        state before the last sync.
        """
        index_local_timestamp = self.get_index_local_timestamp(key)
        real_local_timestamp = self.get_real_local_timestamp(key)
        remote_timestamp = self.get_remote_timestamp(key)

        if index_local_timestamp is None and real_local_timestamp:
            return SyncState(SyncState.UPDATED, real_local_timestamp)
        elif real_local_timestamp is None and index_local_timestamp:
            return SyncState(SyncState.DELETED, remote_timestamp)
        elif real_local_timestamp is None and index_local_timestamp is None and remote_timestamp:
            return SyncState(SyncState.DELETED, remote_timestamp)
        elif real_local_timestamp is None and index_local_timestamp is None:
            # Does not exist in this case, so no action to perform
            return SyncState(SyncState.DOESNOTEXIST, None)
        elif index_local_timestamp < real_local_timestamp:
            return SyncState(SyncState.UPDATED, real_local_timestamp)
        elif index_local_timestamp > real_local_timestamp:
            return SyncState(SyncState.CONFLICT, index_local_timestamp)   # corruption?
        else:
            return SyncState(SyncState.NOCHANGES, remote_timestamp)
