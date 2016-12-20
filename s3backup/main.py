# -*- coding: utf-8 -*-

from s3backup.clients import compare, LocalSyncClient


def sync():
    local_client = LocalSyncClient('/home/michael/Notebooks')
    current = local_client.get_current_state()
    index = local_client.get_index_state()
    print(list(compare(current, index)))
    local_client.update_index()
