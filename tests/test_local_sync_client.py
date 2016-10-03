# -*- coding: utf-8 -*-

import hashlib
import io
import json
import os
import shutil
import tempfile

from s3backup import local_sync_client


class TestGenerateIndex(object):

    def setup_method(self):
        self.local_dir = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.local_dir)

    def test_empty_directory(self):
        assert local_sync_client.generate_index(self.local_dir) == {}

    def test_correct_output(self):
        body = b'hey, whats up?'
        md5 = hashlib.md5()
        md5.update(body)

        with open(os.path.join(self.local_dir, 'foobar'), 'wb') as fp:
            fp.write(body)

        actual_index = local_sync_client.generate_index(self.local_dir)

        assert set(actual_index.keys()) == {'foobar'}
        assert actual_index['foobar']['timestamp'] is None
        # cannot assert exact LastModified
        assert isinstance(actual_index['foobar']['LastModified'], float)
        assert actual_index['foobar']['md5'] == md5.hexdigest()
        assert actual_index['foobar']['size'] == len(body)


def setup_sync_client(object_list, local_dir):
    for key, timestamp in object_list.items():
        path = '{}/{}'.format(local_dir, key)
        local_sync_client.create_parent_directories(path)

        with open(path, 'w'):
            os.utime(path, None)

    with open(os.path.join(local_dir, '.syncindex'), 'w') as fp:
        json.dump(object_list, fp)

    return local_sync_client.LocalSyncClient(local_dir)


class TestLocalSyncClient(object):

    def setup_method(self):
        self.local_dir = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.local_dir)

    def test_creates_folder(self):
        self.local_dir + "/foo/bar"
        local_sync_client.LocalSyncClient(self.local_dir)
        assert os.path.exists(self.local_dir)

    def test_detect_deletions(self):
        with open(os.path.join(self.local_dir, 'foo'), 'w') as fp:
            fp.write('testing')

        with open(os.path.join(self.local_dir, '.syncindex'), 'w') as fp:
            json.dump({
                'foo': {'timestamp': 20323, 'LastModified': 20323, 'size': 20, 'md5': None},
                'bar': {'timestamp': 33333, 'LastModified': 33333, 'size': 40, 'md5': None},
            }, fp)

        local_sync_client.LocalSyncClient(self.local_dir)

    def test_keys_and_timestamp(self):
        sync_client = setup_sync_client({
            'foo': {'timestamp': 1000},
            'bar': {'timestamp': 2000},
            '.hidden': {'timestamp': 4000},
            'hello/world.txt': {'timestamp': 3000},
            '.syncindex.json.gz': {'timestamp': 4500},
            'hello/.hidden': {'timestamp': 5000},
            'hello/.syncindex': {'timestamp': 5000},
        }, self.local_dir)
        assert set(sync_client.keys()) == {
            'foo', 'bar', 'hello/world.txt', '.hidden', 'hello/.hidden'
        }

        assert sync_client.get_object_timestamp('foo') == 1000
        assert sync_client.get_object_timestamp('bar') == 2000
        assert sync_client.get_object_timestamp('.hidden') == 4000
        assert sync_client.get_object_timestamp('hello/world.txt') == 3000
        assert sync_client.get_object_timestamp('hello/.hidden') == 5000

    def test_put_get_object(self):
        sync_client = setup_sync_client({}, self.local_dir)
        sync_client.put_object('foo/hello_world.txt', io.BytesIO(b'howdy'), 10000)
        sync_client.put_object('foobar.md', io.BytesIO(b'baz'), 2023230)

        _, data, _ = sync_client.get_object('foo/hello_world.txt')
        assert data.read() == b'howdy'
        assert sync_client.get_object_timestamp('foo/hello_world.txt') == 10000

        _, data, _ = sync_client.get_object('foobar.md')
        assert data.read() == b'baz'
        assert sync_client.get_object_timestamp('foobar.md') == 2023230
