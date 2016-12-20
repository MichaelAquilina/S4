# -*- coding: utf-8 -*-

import os

from s3backup.clients import compare, LocalSyncClient


def sync():
    target_folder = os.path.expanduser('~/Notebooks')

    local_client = LocalSyncClient(target_folder)
    current = local_client.get_current_state()
    index = local_client.get_index_state()
    print(list(compare(current, index)))
    local_client.update_index()
