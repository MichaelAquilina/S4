# -*- coding: utf-8 -*-

import os

from s3backup.clients.local import LocalSyncClient
from s3backup.sync import compare_states


def sync():
    target_folder = os.path.expanduser('~/Notebooks')

    local_client = LocalSyncClient(target_folder)
    current = local_client.get_current_state()
    index = local_client.get_index_state()
    print(list(compare_states(current, index)))
    local_client.update_index()
