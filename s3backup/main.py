# -*- coding: utf-8 -*-

from s3backup.clients import compare, LocalSyncClient


def sync():
    local_client = LocalSyncClient('/home/michael/Notebooks')
    # print(local_client.index())
    # print(local_client.current())
    print(list(compare(local_client.index(), local_client.current())))
    local_client.save_index()
