# -*- coding: utf-8 -*-

import io
import os
import tempfile

from s3backup.local_sync_client import LocalSyncClient


def setup_sync_client(object_names):
    local_dir = tempfile.mkdtemp()
    for object_name in object_names:
        file_name = '{}/{}'.format(local_dir, object_name)
        with open(file_name, 'w'):
            os.utime(file_name, None)
    return LocalSyncClient(local_dir)


class TestLocalSyncClient(object):

    def test_keys(self):
        sync_client = setup_sync_client(['foo', 'bar'])
        assert set(sync_client.keys()) == {'foo', 'bar'}

    def test_put_object(self):
        fp = io.BytesIO(b'howdy')

        sync_client = setup_sync_client([])
        sync_client.put_object('foo/hello_world.txt', fp, 10000)

        assert sync_client.get_object_timestamp('foo/hello_world.txt') == 10000
