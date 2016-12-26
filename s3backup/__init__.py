# -*- coding: utf-8 -*-

import enum


class StateAction(enum.Enum):
    CREATE = 'CREATE'
    DELETE = 'DELETE'
    UPDATE = 'UPDATE'
    CONFLICT = 'CONFLICT'


class SyncAction(enum.Enum):
    DOWNLOAD = 'DOWNLOAD'
    DELETE_LOCAL = 'DELETE_LOCAL'
    DELETE_REMOTE = 'DELETE_REMOTE'
    UPLOAD = 'UPLOAD'
    CONFLICT = 'CONFLICT'


def compare_states(current, previous):
    all_keys = set(previous.keys()) | set(current.keys())
    for key in all_keys:
        in_previous = key in previous
        in_current = key in current
        if in_previous and in_current:
            previous_timestamp = previous[key]['local_timestamp']
            current_timestamp = current[key]['local_timestamp']
            if previous_timestamp == current_timestamp:
                yield key, None
            elif previous_timestamp < current_timestamp:
                yield key, StateAction.UPDATE
            elif previous_timestamp > current_timestamp:
                yield key, StateAction.CONFLICT
        elif in_current and not in_previous:
            yield key, StateAction.CREATE
        elif in_previous and not in_current:
            yield key, StateAction.DELETE
        else:
            raise ValueError('Reached Unknown state')


def compare_actions(actions_1, actions_2):
    all_keys = set(actions_1.keys() | actions_2.keys())
    for key in all_keys:
        a1 = actions_1.get(key)
        a2 = actions_2.get(key)

        if a1 is None and a2 is None:
            continue

        if a1 is None and a2 == StateAction.CREATE:
            yield key, SyncAction.DOWNLOAD

        elif a1 == StateAction.CREATE and a2 is None:
            yield key, SyncAction.UPLOAD

        elif a1 is None and a2 == StateAction.UPDATE:
            yield key, SyncAction.DOWNLOAD

        elif a1 == StateAction.UPDATE and a2 is None:
            yield key, SyncAction.UPLOAD

        elif a1 is None and a2 == StateAction.DELETE:
            yield key, SyncAction.DELETE_LOCAL

        elif a1 == StateAction.DELETE and a2 is None:
            yield key, SyncAction.DELETE_REMOTE

        else:
            yield key, SyncAction.CONFLICT


def sync(client_1, client_2):
    current = client_2.get_current_state()
    index = client_2.get_index_state()
    actions_2 = dict(compare_states(current, index))

    current = client_1.get_current_state()
    index = client_1.get_index_state()
    actions_1 = dict(compare_states(current, index))

    for key, action in compare_actions(actions_1, actions_2):
        if action == SyncAction.DOWNLOAD:
            print('Downloading', key)
            client_1.put(key, client_2.get(key))
        elif action == SyncAction.UPLOAD:
            print('Uploading', key)
            client_2.put(key, client_1.get(key))
        elif action == SyncAction.DELETE_LOCAL:
            print('Delete local', key)
            client_1.delete(key)
        elif action == SyncAction.DELETE_REMOTE:
            print('Delete remote', key)
            client_2.delete(key)
        elif action == SyncAction.CONFLICT:
            print('Need to resolve Conflict for', key)
        else:
            raise ValueError('You should never reach here')

    print('Updating Indexes')
    client_2.update_index()
    client_1.update_index()
