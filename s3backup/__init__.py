# -*- coding: utf-8 -*-

import enum

import boto3

from s3backup.clients import s3, local


class IndexAction(enum.Enum):
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
            previous_timestamp = previous[key]['timestamp']
            current_timestamp = current[key]['timestamp']
            if previous_timestamp == current_timestamp:
                yield key, None
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


def compare_actions(actions_1, actions_2):
    all_keys = set(actions_1.keys() | actions_2.keys())
    for key in all_keys:
        a1 = actions_1.get(key)
        a2 = actions_2.get(key)

        if a1 is None and a2 is None:
            continue

        if a1 is None and a2 == IndexAction.CREATE:
            yield key, SyncAction.DOWNLOAD

        elif a1 == IndexAction.CREATE and a2 is None:
            yield key, SyncAction.UPLOAD

        elif a1 is None and a2 == IndexAction.UPDATE:
            yield key, SyncAction.DOWNLOAD

        elif a1 == IndexAction.UPDATE and a2 is None:
            yield key, SyncAction.UPLOAD

        elif a1 is None and a2 == IndexAction.DELETE:
            yield key, SyncAction.DELETE_LOCAL

        elif a1 == IndexAction.DELETE and a2 is None:
            yield key, SyncAction.DELETE_REMOTE

        else:
            yield key, SyncAction.CONFLICT


def sync(target_folder, bucket, prefix):
    client = boto3.client('s3')

    local_client = local.LocalSyncClient(target_folder)
    s3_client = s3.S3SyncClient(client, bucket, prefix)

    current = s3_client.get_current_state()
    index = s3_client.get_index_state()
    s3_actions = dict(compare_states(current, index))

    current = local_client.get_current_state()
    index = local_client.get_index_state()
    local_actions = dict(compare_states(current, index))

    for key, action in compare_actions(local_actions, s3_actions):
        if action == SyncAction.DOWNLOAD:
            print('Downloading', key)
            local_client.put(key, s3_client.get(key))
        elif action == SyncAction.UPLOAD:
            print('Uploading', key)
            s3_client.put(key, local_client.get(key))
        elif action == SyncAction.DELETE_LOCAL:
            print('Delete local', key)
            local_client.delete(key)
        elif action == SyncAction.DELETE_REMOTE:
            print('Delete remote', key)
            s3_client.delete(key)
        elif action == SyncAction.CONFLICT:
            print('Need to resolve Conflict for', key)
        else:
            raise ValueError('You should never reach here')

    print('Updating Indexes')
    s3_client.update_index()
    local_client.update_index()
