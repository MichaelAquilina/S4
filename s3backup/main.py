# -*- coding: utf-8 -*-

import os

import boto3

from s3backup.clients.local import LocalSyncClient
from s3backup.clients.s3 import S3SyncClient
from s3backup.sync import SyncAction, compare_actions, compare_states


def sync():
    target_folder = os.path.expanduser('~/Notebooks')

    bucket = 'michaelaquilina.data2'
    prefix = 'ZimNoteBooks2'
    client = boto3.client('s3')

    local_client = LocalSyncClient(target_folder)
    s3_client = S3SyncClient(client, bucket, prefix)

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
