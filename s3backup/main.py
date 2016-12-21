# -*- coding: utf-8 -*-

import os

import boto3

from s3backup.clients.local import LocalSyncClient
from s3backup.clients.s3 import S3SyncClient
from s3backup.sync import compare_states


def sync():
    target_folder = os.path.expanduser('~/Notebooks')

    bucket = 'michaelaquilina.data2'
    prefix = 'ZimNoteBooks'
    client = boto3.client('s3')

    local_client = LocalSyncClient(target_folder)
    s3_client = S3SyncClient(client, bucket, prefix)

    current = s3_client.get_current_state()
    index = s3_client.get_index_state()
    print(list(compare_states(current, index)))

    current = local_client.get_current_state()
    index = local_client.get_index_state()
    print(list(compare_states(current, index)))
