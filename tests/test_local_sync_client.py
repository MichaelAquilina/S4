# -*- coding: utf-8 -*-

import os
import tempfile

from s3backup.local_sync_client import LocalSyncClient


class TestLocalSyncClient(object):

    def test_keys(self):
        local_dir = tempfile.mkdtemp()
        for object_name in ('foo', 'bar'):
            file_name = '{}/{}'.format(local_dir, object_name)
            with open(file_name, 'w'):
                os.utime(file_name, None)

        sync_client = LocalSyncClient(local_dir)
        assert set(sync_client.keys()) == {'foo', 'bar'}
