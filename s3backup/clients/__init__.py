# -*- coding: utf-8 -*-

import datetime


class SyncState(object):
    UPDATED = 'UPDATED'
    CREATED = 'CREATED'
    DELETED = 'DELETED'
    CONFLICT = 'CONFLICT'
    NOCHANGES = 'NOCHANGES'
    DOESNOTEXIST = 'DOESNOTEXIST'

    def __init__(self, action, timestamp):
        self.action = action
        self.timestamp = timestamp

    def get_datetime(self):
        if self.timestamp is not None:
            return datetime.datetime.utcfromtimestamp(self.timestamp)
        else:
            return None

    def __eq__(self, other):
        if not isinstance(other, SyncState):
            return False
        return self.action == other.action and self.timestamp == other.timestamp

    def __repr__(self):
        return 'SyncState<{}, {}>'.format(self.action, self.get_datetime())


class SyncObject(object):
    def __init__(self, fp, total_size, timestamp):
        self.fp = fp
        self.total_size = total_size
        self.timestamp = timestamp

    def __repr__(self):
        return 'SyncObject<{}, {}, {}>'.format(self.fp, self.total_size, self.timestamp)


def get_sync_state(index_local, real_local, remote):
    if index_local is None and real_local:
        return SyncState(SyncState.CREATED, real_local)
    elif real_local is None and index_local:
        return SyncState(SyncState.DELETED, remote)
    elif real_local is None and index_local is None and remote:
        return SyncState(SyncState.DELETED, remote)
    elif real_local is None and index_local is None:
        # Does not exist in this case, so no action to perform
        return SyncState(SyncState.DOESNOTEXIST, None)

    # convert to int because not all clients support float precision
    elif int(index_local) < int(real_local):
        return SyncState(SyncState.UPDATED, real_local)
    elif int(index_local) > int(real_local):
        return SyncState(SyncState.CONFLICT, index_local)   # corruption?
    else:
        return SyncState(SyncState.NOCHANGES, remote)


class SyncClient(object):
    def get_uri(self):
        raise NotImplemented()

    def put(self, key, sync_object):
        raise NotImplemented()

    def get(self, key):
        raise NotImplemented()

    def delete(self, key):
        raise NotImplemented()

    def get_local_keys(self):
        raise NotImplemented()

    def get_real_local_timestamp(self, key):
        raise NotImplemented()

    def get_index_keys(self):
        raise NotImplemented()

    def get_index_local_timestamp(self, key):
        raise NotImplemented()

    def set_index_local_timestamp(self, key, timestamp):
        raise NotImplemented()

    def get_remote_timestamp(self, key):
        raise NotImplemented()

    def set_remote_timestamp(self, key, timestamp):
        raise NotImplemented()

    def get_all_remote_timestamps(self):
        raise NotImplemented()

    def get_all_index_local_timestamps(self):
        raise NotImplemented()

    def get_all_real_local_timestamps(self):
        raise NotImplemented()

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
        raise NotImplemented()

    def get_action(self, key):
        """
        returns the action to perform on this key based on its
        state before the last sync.
        """
        index_local_timestamp = self.get_index_local_timestamp(key)
        real_local_timestamp = self.get_real_local_timestamp(key)
        remote_timestamp = self.get_remote_timestamp(key)
        return get_sync_state(index_local_timestamp, real_local_timestamp, remote_timestamp)

    def get_actions(self, keys):
        real_local_timestamps = self.get_all_real_local_timestamps()
        index_local_timestamps = self.get_all_index_local_timestamps()
        remote_timestamps = self.get_all_remote_timestamps()

        results = {}
        for key in keys:
            index_local_timestamp = index_local_timestamps.get(key)
            real_local_timestamp = real_local_timestamps.get(key)
            remote_timestamp = remote_timestamps.get(key)

            results[key] = get_sync_state(
                index_local_timestamp,
                real_local_timestamp,
                remote_timestamp,
            )

        return results
