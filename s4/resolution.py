# -*- encoding: utf-8 -*-

from s4.clients import SyncState


class Resolution(object):
    UPDATE = 'UPDATE'
    CREATE = 'CREATE'
    DELETE = 'DELETE'

    def __init__(self, action, to_client, from_client, key, timestamp):
        self.action = action
        self.to_client = to_client
        self.from_client = from_client
        self.key = key
        self.timestamp = timestamp

    def __eq__(self, other):
        if not isinstance(other, Resolution):
            return False
        return (
            self.action == other.action and
            self.to_client == other.to_client and
            self.from_client == other.from_client and
            self.key == other.key and
            self.timestamp == other.timestamp
        )

    def __repr__(self):
        return 'Resolution<action={}, to={}, from={}, key={}, timestamp={}>'.format(
            self.action,
            self.to_client.get_uri() if self.to_client is not None else None,
            self.from_client.get_uri() if self.from_client is not None else None,
            self.key,
            self.timestamp,
        )

    @staticmethod
    def get_resolution(key, sync_state, to_client, from_client):
        if sync_state.state in (SyncState.UPDATED, SyncState.NOCHANGES):
            return Resolution(
                Resolution.UPDATE, to_client, from_client, key, sync_state.local_timestamp
            )
        elif sync_state.state == SyncState.CREATED:
            return Resolution(
                Resolution.CREATE, to_client, from_client, key, sync_state.local_timestamp
            )
        elif sync_state.state == SyncState.DELETED:
            return Resolution(Resolution.DELETE, to_client, None, key, sync_state.remote_timestamp)
        else:
            raise ValueError('Unknown action provided', sync_state)
