# -*- coding: utf-8 -*-

import os
import shutil
import tempfile

import s3backup
from s3backup.clients.local import LocalSyncClient


def get_pairs(list_of_things):
    for i in range(len(list_of_things)):
        yield list_of_things[i], list_of_things[(i + 1) % len(list_of_things)]


class TestGetPairs(object):
    def test_empty(self):
        assert list(get_pairs([])) == []

    def test_correct_output(self):
        assert list(get_pairs(['a', 'b', 'c'])) == [('a', 'b'), ('b', 'c'), ('c', 'a')]


class TestSync(object):
    def setup_method(self):
        self.clients = []
        self.folders = []

    def teardown_method(self):
        for folder in self.folders:
            shutil.rmtree(folder)

    def create_clients(self, n):
        for _ in range(n):
            folder = tempfile.mkdtemp()
            client = LocalSyncClient(folder)
            self.folders.append(folder)
            self.clients.append(client)

    def sync_clients(self):
        for client_1, client_2 in get_pairs(self.clients):
            s3backup.sync(client_1, client_2)

    def set_contents(self, folder, key, timestamp=None, data=''):
        path = os.path.join(folder, key)
        with open(path, 'w') as fp:
            fp.write(data)
        if timestamp is not None:
            os.utime(path, (timestamp, timestamp))

    def delete(self, folder, key):
        os.remove(os.path.join(folder, key))

    def assert_file_existence(self, keys, exists):
        for folder in self.folders:
            for key in keys:
                assert os.path.exists(os.path.join(folder, key)) is exists

    def assert_contents(self, key, data):
        for folder in self.folders:
            path = os.path.join(folder, key)
            with open(path, 'r') as fp:
                assert data == fp.read()

    def assert_remote_timestamp(self, key, expected_timestamp):
        for client in self.clients:
            assert client.get_remote_timestamp(key) == expected_timestamp

    def assert_local_keys(self, expected_keys):
        for client in self.clients:
            assert sorted(client.get_local_keys()) == sorted(expected_keys)

    def test_fresh_sync(self):
        self.create_clients(2)

        self.set_contents(self.folders[0], 'foo', timestamp=1000)
        self.set_contents(self.folders[0], 'bar', timestamp=2000)
        self.set_contents(self.folders[1], 'baz', timestamp=3000, data='what is up?')

        self.sync_clients()

        self.assert_local_keys(['foo', 'bar', 'baz'])
        self.assert_remote_timestamp('foo', 1000)
        self.assert_remote_timestamp('bar', 2000)
        self.assert_remote_timestamp('baz', 3000)
        self.assert_file_existence(['foo', 'bar', 'baz'], True)
        self.assert_contents('baz', 'what is up?')

        self.delete(self.folders[0], 'foo')
        self.set_contents(self.folders[0], 'test', timestamp=5000)
        self.set_contents(self.folders[1], 'hello', timestamp=6000)
        self.set_contents(self.folders[1], 'baz', timestamp=8000, data='just syncing some stuff')

        self.sync_clients()

        self.assert_file_existence(['foo'], False)
        self.assert_local_keys(['test', 'bar', 'baz', 'hello'])
        self.assert_remote_timestamp('bar', 2000)
        self.assert_remote_timestamp('test', 5000)
        self.assert_remote_timestamp('hello', 6000)
        self.assert_remote_timestamp('baz', 8000)
        self.assert_contents('baz', 'just syncing some stuff')

        self.sync_clients()

    def test_three_way_sync(self):
        self.create_clients(3)

        self.set_contents(self.folders[0], 'foo', timestamp=1000)
        self.set_contents(self.folders[1], 'bar', timestamp=2000, data='red')
        self.set_contents(self.folders[2], 'baz', timestamp=3000)

        self.sync_clients()

        self.assert_local_keys(['foo', 'bar', 'baz'])
        self.assert_contents('bar', 'red')
        self.assert_remote_timestamp('foo', 1000)
        self.assert_remote_timestamp('bar', 2000)
        self.assert_remote_timestamp('baz', 3000)

        self.set_contents(self.folders[1], 'bar', timestamp=8000, data='green')
        self.sync_clients()

        self.assert_local_keys(['foo', 'bar', 'baz'])
        self.assert_contents('bar', 'green')
        self.assert_remote_timestamp('foo', 1000)
        self.assert_remote_timestamp('bar', 8000)
        self.assert_remote_timestamp('baz', 3000)

        self.delete(self.folders[2], 'foo')
        self.sync_clients()

        self.assert_file_existence(['foo'], False)
        self.assert_local_keys(['bar', 'baz'])

        self.sync_clients()
