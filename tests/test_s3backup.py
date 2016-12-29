# -*- coding: utf-8 -*-

import os
import shutil
import tempfile

import s3backup
from s3backup.clients.local import LocalSyncClient


class TestSync(object):
    def setup_method(self):
        self.folder_1 = tempfile.mkdtemp()
        self.client_1 = LocalSyncClient(self.folder_1)

        self.folder_2 = tempfile.mkdtemp()
        self.client_2 = LocalSyncClient(self.folder_2)

    def teardown_method(self):
        shutil.rmtree(self.folder_1)
        shutil.rmtree(self.folder_2)

    def set_contents(self, folder, key, timestamp=None, data=''):
        path = os.path.join(folder, key)
        with open(path, 'w') as fp:
            fp.write(data)
        if timestamp is not None:
            os.utime(path, (timestamp, timestamp))

    def delete(self, folder, key):
        os.remove(os.path.join(folder, key))

    def assert_contents(self, folders, key, data):
        for folder in folders:
            path = os.path.join(folder, key)
            with open(path, 'r') as fp:
                assert data == fp.read()

    def assert_remote_timestamp(self, clients, key, timestamp):
        for client in clients:
            assert client.get_remote_timestamp(key) == timestamp

    def test_fresh_sync(self):
        self.set_contents(self.folder_1, 'foo', timestamp=1000)
        self.set_contents(self.folder_1, 'bar', timestamp=2000)
        self.set_contents(self.folder_2, 'baz', timestamp=3000, data='what is up?')

        s3backup.sync(self.client_1, self.client_2)

        expected_keys = sorted(['foo', 'bar', 'baz'])

        assert sorted(self.client_1.get_local_keys()) == expected_keys
        assert sorted(self.client_2.get_local_keys()) == expected_keys

        self.assert_remote_timestamp([self.client_1, self.client_2], 'foo', 1000)
        self.assert_remote_timestamp([self.client_1, self.client_2], 'bar', 2000)
        self.assert_remote_timestamp([self.client_1, self.client_2], 'baz', 3000)
        self.assert_contents([self.folder_1, self.folder_2], 'baz', 'what is up?')

        self.delete(self.folder_1, 'foo')
        self.set_contents(self.folder_1, 'test', timestamp=5000)
        self.set_contents(self.folder_2, 'hello', timestamp=6000)
        self.set_contents(self.folder_2, 'baz', timestamp=8000, data='just syncing some stuff')

        s3backup.sync(self.client_1, self.client_2)

        expected_keys = sorted(['test', 'bar', 'baz', 'hello'])

        assert os.path.exists(os.path.join(self.folder_1, 'foo')) is False
        assert os.path.exists(os.path.join(self.folder_2, 'foo')) is False

        assert sorted(self.client_1.get_local_keys()) == expected_keys
        assert sorted(self.client_2.get_local_keys()) == expected_keys

        self.assert_remote_timestamp([self.client_1, self.client_2], 'bar', 2000)
        self.assert_remote_timestamp([self.client_1, self.client_2], 'test', 5000)
        self.assert_remote_timestamp([self.client_1, self.client_2], 'hello', 6000)
        self.assert_remote_timestamp([self.client_1, self.client_2], 'baz', 8000)
        self.assert_contents([self.folder_1, self.folder_2], 'baz', 'just syncing some stuff')
