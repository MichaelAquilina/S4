# -*- coding: utf-8 -*-

import io
import os
import tempfile

from s3backup import local_sync_client


def setup_sync_client(object_names):
    local_dir = tempfile.mkdtemp()
    for key in object_names:
        path = '{}/{}'.format(local_dir, key)
        local_sync_client.create_parent_directories(path)

        with open(path, 'w'):
            os.utime(path, None)
    return local_sync_client.LocalSyncClient(local_dir)


class TestLocalSyncClient(object):

    def test_keys(self):
        sync_client = setup_sync_client([
            'foo', 'bar', 'hello/world.txt', '.hidden', 'hello/.hidden'
        ])
        assert set(sync_client.keys()) == {'foo', 'bar', 'hello/world.txt'}

    def test_put_object(self):
        sync_client = setup_sync_client([])
        sync_client.put_object('foo/hello_world.txt', io.BytesIO(b'howdy'), 10000)
        sync_client.put_object('foobar.md', io.BytesIO(b'baz'), 2023230)

        assert sync_client.get_object_timestamp('foo/hello_world.txt') == 10000
        assert sync_client.get_object_timestamp('foobar.md') == 2023230
