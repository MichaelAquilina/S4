# -*- coding: utf-8 -*-

import io
import os
import tempfile

from s3backup import local_sync_client


def setup_sync_client(object_list):
    local_dir = tempfile.mkdtemp()
    for key, timestamp in object_list.items():
        path = '{}/{}'.format(local_dir, key)
        local_sync_client.create_parent_directories(path)

        with open(path, 'w'):
            os.utime(path, (timestamp, timestamp))
    return local_sync_client.LocalSyncClient(local_dir)


class TestLocalSyncClient(object):

    def test_keys_and_timestamp(self):
        sync_client = setup_sync_client({
            'foo': 1000,
            'bar': 2000,
            '.hidden': 4000,
            'hello/world.txt': 3000,
            '.syncindex.json.gz': 4500,
            'hello/.hidden': 5000,
            'hello/.syncindex': 5000,
        })
        assert set(sync_client.keys()) == {
            'foo', 'bar', 'hello/world.txt', '.hidden', 'hello/.hidden'
        }

        assert sync_client.get_object_timestamp('foo') == 1000
        assert sync_client.get_object_timestamp('bar') == 2000
        assert sync_client.get_object_timestamp('.hidden') == 4000
        assert sync_client.get_object_timestamp('hello/world.txt') == 3000
        assert sync_client.get_object_timestamp('hello/.hidden') == 5000

    def test_put_get_object(self):
        sync_client = setup_sync_client({})
        sync_client.put_object('foo/hello_world.txt', io.BytesIO(b'howdy'), 10000)
        sync_client.put_object('foobar.md', io.BytesIO(b'baz'), 2023230)

        assert sync_client.get_object('foo/hello_world.txt').read() == b'howdy'
        assert sync_client.get_object_timestamp('foo/hello_world.txt') == 10000

        assert sync_client.get_object('foobar.md').read() == b'baz'
        assert sync_client.get_object_timestamp('foobar.md') == 2023230
