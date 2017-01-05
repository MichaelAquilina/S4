# -*- coding: utf-8 -*-

import datetime


class SyncAction(object):
    UPDATED = 'UPDATED'
    DELETED = 'DELETED'
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


class SyncClient(object):
    def put(self, key, sync_object):
        raise NotImplemented

    def get(self, key):
        raise NotImplemented

    def delete(self, key):
        raise NotImplemented

    def update_index(self):
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

    def get_action(self, key):
        """
        returns the action to perform on this key based on its
        state before the last sync.
        """
        index_local_timestamp = self.get_index_local_timestamp(key)
        real_local_timestamp = self.get_real_local_timestamp(key)
        remote_timestamp = self.get_remote_timestamp(key)

        if index_local_timestamp is None and real_local_timestamp:
            return SyncAction(SyncAction.UPDATED, real_local_timestamp)
        elif real_local_timestamp is None and index_local_timestamp:
            return SyncAction(SyncAction.DELETED, remote_timestamp)
        elif real_local_timestamp is None and index_local_timestamp is None and remote_timestamp:
            return SyncAction(SyncAction.DELETED, remote_timestamp)
        elif real_local_timestamp is None and index_local_timestamp is None:
            # Does not exist in this case, so no action to perform
            return SyncAction(SyncAction.NONE, None)
        elif index_local_timestamp < real_local_timestamp:
            return SyncAction(SyncAction.UPDATED, real_local_timestamp)
        elif index_local_timestamp > real_local_timestamp:
            return SyncAction(SyncAction.CONFLICT, index_local_timestamp)   # corruption?
        else:
            return SyncAction(SyncAction.NONE, remote_timestamp)
