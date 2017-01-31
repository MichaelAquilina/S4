# -*- coding: utf-8 -*-

import datetime
import json
import os
import shutil
import tempfile

import boto3

import freezegun

from moto import mock_s3

from s3backup import sync
from s3backup.clients import SyncState
from s3backup.clients.local import LocalSyncClient
from s3backup.clients.s3 import S3SyncClient


def get_pairs(list_of_things):
    for i in range(len(list_of_things)):
        yield list_of_things[i], list_of_things[(i + 1) % len(list_of_things)]


class TestGetPairs(object):
    def test_empty(self):
        assert list(get_pairs([])) == []

    def test_correct_output(self):
        assert list(get_pairs(['a', 'b', 'c'])) == [('a', 'b'), ('b', 'c'), ('c', 'a')]


def set_local_contents(client, key, timestamp=None, data=''):
    path = os.path.join(client.path, key)
    parent = os.path.dirname(path)
    if not os.path.exists(parent):
        os.makedirs(parent)
    with open(path, 'w') as fp:
        fp.write(data)
    if timestamp is not None:
        os.utime(path, (timestamp, timestamp))


def get_local_contents(local_client, key):
    path = os.path.join(local_client.path, key)
    with open(path, 'r') as fp:
        data = fp.read()
    return data


def set_local_index(local_client, data):
    with open(os.path.join(local_client.path, '.index'), 'w') as fp:
        json.dump(data, fp)


def delete_local(client, key):
    os.remove(os.path.join(client.path, key))


def set_s3_contents(s3_client, key, timestamp=None, data=''):
    if timestamp is None:
        freeze_time = datetime.datetime.utcnow()
    else:
        freeze_time = datetime.datetime.utcfromtimestamp(timestamp)

    with freezegun.freeze_time(freeze_time):
        s3_client.client.put_object(
            Bucket=s3_client.bucket,
            Key=os.path.join(s3_client.prefix, key),
            Body=data,
        )


def set_s3_index(s3_client, data):
    s3_client.client.put_object(
        Bucket=s3_client.bucket,
        Key=os.path.join(s3_client.prefix, '.index'),
        Body=json.dumps(data),
    )


class TestGetActions(object):
    def test_empty_clients(self, s3_client, local_client):
        actual_output = list(sync.get_actions(s3_client, local_client))
        assert actual_output == []


class TestGetSyncActions(object):
    def test_empty(self, local_client, s3_client):
        assert sync.get_sync_actions(local_client, s3_client) == ({}, {})

    def test_correct_output(self, local_client, s3_client):
        set_local_index(local_client, {
            'chemistry.txt': {
                'local_timestamp': 9431,
                'remote_timestamp': 9431,
            },
            'physics.txt': {
                'local_timestamp': 10000,
                'remote_timestamp': 10000,
            },
            'maltese.txt': {
                'local_timestamp': 7000,
                'remote_timestamp': 6000,
            },
        })
        set_s3_index(s3_client, {
            'chemistry.txt': {
                'local_timestamp': 10000,
                'remote_timestamp': 9431,
            },
            'physics.txt': {
                'local_timestamp': 13000,
                'remote_timestamp': 12000,
            },
            'maltese.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            },
        })

        set_local_contents(local_client, 'history.txt', timestamp=5000)
        set_s3_contents(s3_client, 'art.txt', timestamp=200000)
        set_local_contents(local_client, 'english.txt', timestamp=90000)
        set_s3_contents(s3_client, 'english.txt', timestamp=93000)
        set_s3_contents(s3_client, 'chemistry.txt', timestamp=10000)
        set_local_contents(local_client, 'physics.txt', timestamp=11000)
        set_s3_contents(s3_client, 'physics.txt', timestamp=13000)
        set_local_contents(local_client, 'maltese.txt', timestamp=7000)
        set_s3_contents(s3_client, 'maltese.txt', timestamp=8000)

        local_client.reload_index()
        s3_client.reload_index()
        deferred_calls, unhandled_events = sync.get_sync_actions(local_client, s3_client)
        expected_unhandled_events = {
            'english.txt': (
                SyncState(SyncState.CREATED, 90000, None),
                SyncState(SyncState.CREATED, 93000, None)
            ),
            'physics.txt': (
                SyncState(SyncState.UPDATED, 11000, 10000),
                SyncState(SyncState.NOCHANGES, 13000, 12000),
            )
        }
        expected_deferred_calls = {
            'maltese.txt': sync.DeferredFunction(
                sync.update_client, local_client, s3_client, 'maltese.txt', 8000
            ),
            'chemistry.txt': sync.DeferredFunction(
                sync.delete_client, s3_client, 'chemistry.txt', 9431
            ),
            'history.txt': sync.DeferredFunction(
                sync.create_client, s3_client, local_client, 'history.txt', 5000
            ),
            'art.txt': sync.DeferredFunction(
                sync.create_client, local_client, s3_client, 'art.txt', 200000
            ),
        }
        assert unhandled_events == expected_unhandled_events
        assert deferred_calls == expected_deferred_calls

    def test_nochanges_but_different_remote_timestamps(self, local_client, s3_client):
        set_local_index(local_client, {
            'german.txt': {
                'local_timestamp': 4000,
                'remote_timestamp': 4000,
            }
        })
        set_s3_index(s3_client, {
            'german.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            }
        })
        set_local_contents(local_client, 'german.txt', timestamp=4000)
        set_s3_contents(s3_client, 'german.txt', timestamp=6000)

        local_client.reload_index()
        s3_client.reload_index()
        deferred_calls, unhandled_events = sync.get_sync_actions(local_client, s3_client)
        expected_deferred_calls = {
            'german.txt': sync.DeferredFunction(
                sync.update_client, local_client, s3_client, 'german.txt', 6000)
        }
        assert deferred_calls == expected_deferred_calls
        assert unhandled_events == {}

    def test_updated_but_different_remote_timestamp(self, local_client, s3_client):
        set_local_index(local_client, {
            'biology.txt': {
                'local_timestamp': 4000,
                'remote_timestamp': 3000,
            }
        })
        set_s3_index(s3_client, {
            'biology.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            }
        })
        set_local_contents(local_client, 'biology.txt', timestamp=4500)
        set_s3_contents(s3_client, 'biology.txt', timestamp=6000)

        local_client.reload_index()
        s3_client.reload_index()
        deferred_calls, unhandled_events = sync.get_sync_actions(local_client, s3_client)
        expected_unhandled_events = {
            'biology.txt': (
                SyncState(SyncState.UPDATED, 4500, 3000), SyncState(SyncState.NOCHANGES, 6000, 6000)
            )
        }
        assert deferred_calls == {}
        assert unhandled_events == expected_unhandled_events

    def test_deleted_but_different_remote_timestamp(self, local_client, s3_client):
        set_local_index(local_client, {
            'chemistry.txt': {
                'local_timestamp': 4000,
                'remote_timestamp': 3000,
            }
        })
        set_s3_index(s3_client, {
            'chemistry.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            }
        })
        set_s3_contents(s3_client, 'chemistry.txt', timestamp=6000)

        local_client.reload_index()
        s3_client.reload_index()
        deferred_calls, unhandled_events = sync.get_sync_actions(local_client, s3_client)
        expected_unhandled_events = {
            'chemistry.txt': (
                SyncState(SyncState.DELETED, None, 3000), SyncState(SyncState.NOCHANGES, 6000, 6000)
            )
        }
        assert deferred_calls == {}
        assert unhandled_events == expected_unhandled_events

    def test_deleted_doesnotexist(self, local_client, s3_client):
        set_local_index(local_client, {
            'physics.txt': {
                'local_timestamp': 5000,
                'remote_timestamp': 4550,
            }
        })
        local_client.reload_index()
        deferred_calls, unhandled_events = sync.get_sync_actions(local_client, s3_client)
        assert deferred_calls == {}
        assert unhandled_events == {}


class TestIntegrations(object):
    def setup_method(self):
        self.clients = []
        self.folders = []

    def teardown_method(self):
        for folder in self.folders:
            shutil.rmtree(folder)

    def create_local_client(self):
        folder = tempfile.mkdtemp()
        client = LocalSyncClient(folder)
        self.folders.append(folder)
        self.clients.append(client)
        return client, folder

    def create_s3_client(self, s3_client, bucket, prefix):
        client = S3SyncClient(s3_client, bucket, prefix)
        self.clients.append(client)
        return client

    def sync_clients(self):
        for client_1, client_2 in get_pairs(self.clients):
            sync.sync(client_1, client_2)

    def assert_file_existence(self, keys, exists):
        for folder in self.folders:
            for key in keys:
                assert os.path.exists(os.path.join(folder, key)) is exists

    def assert_contents(self, key, data=None, timestamp=None):
        for client in self.clients:
            sync_object = client.get(key)
            if data is not None:
                assert sync_object.fp.read() == data
            if timestamp is not None:
                assert sync_object.timestamp == timestamp

    def assert_remote_timestamp(self, key, expected_timestamp):
        for client in self.clients:
            assert client.get_remote_timestamp(key) == expected_timestamp

    def assert_local_keys(self, expected_keys):
        for client in self.clients:
            assert sorted(client.get_local_keys()) == sorted(expected_keys)

    @mock_s3
    def test_local_with_s3(self):
        boto_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        boto_client.create_bucket(Bucket='testbucket')
        s3_client = self.create_s3_client(boto_client, 'testbucket', 'asgard')

        set_s3_contents(s3_client, 'colors/cream', 9999, '#ddeeff')

        local_client, _ = self.create_local_client()
        set_local_contents(local_client, 'colors/red', 5000, '#ff0000')
        set_local_contents(local_client, 'colors/green', 3000, '#00ff00')
        set_local_contents(local_client, 'colors/blue', 2000, '#0000ff')

        self.sync_clients()

        expected_keys = [
            'colors/red',
            'colors/green',
            'colors/blue',
            'colors/cream'
        ]
        self.assert_local_keys(expected_keys)
        self.assert_contents('colors/red', b'#ff0000')
        self.assert_contents('colors/green', b'#00ff00')
        self.assert_contents('colors/blue', b'#0000ff')
        self.assert_contents('colors/cream', b'#ddeeff')
        self.assert_remote_timestamp('colors/red', 5000)
        self.assert_remote_timestamp('colors/green', 3000)
        self.assert_remote_timestamp('colors/blue', 2000)
        self.assert_remote_timestamp('colors/cream', 9999)

        delete_local(local_client, 'colors/red')

        self.sync_clients()
        expected_keys = [
            'colors/green',
            'colors/blue',
            'colors/cream'
        ]
        self.assert_local_keys(expected_keys)

    def test_fresh_sync(self):
        self.create_local_client()
        self.create_local_client()

        set_local_contents(self.clients[0], 'foo', timestamp=1000)
        set_local_contents(self.clients[0], 'bar', timestamp=2000)
        set_local_contents(self.clients[1], 'baz', timestamp=3000, data='what is up?')

        self.sync_clients()

        self.assert_local_keys(['foo', 'bar', 'baz'])
        self.assert_remote_timestamp('foo', 1000)
        self.assert_remote_timestamp('bar', 2000)
        self.assert_remote_timestamp('baz', 3000)
        self.assert_file_existence(['foo', 'bar', 'baz'], True)
        self.assert_contents('baz', b'what is up?')

        delete_local(self.clients[0], 'foo')
        set_local_contents(self.clients[0], 'test', timestamp=5000)
        set_local_contents(self.clients[1], 'hello', timestamp=6000)
        set_local_contents(self.clients[1], 'baz', timestamp=8000, data='just syncing some stuff')

        self.sync_clients()

        self.assert_file_existence(['foo'], False)
        self.assert_local_keys(['test', 'bar', 'baz', 'hello'])
        self.assert_remote_timestamp('bar', 2000)
        self.assert_remote_timestamp('test', 5000)
        self.assert_remote_timestamp('hello', 6000)
        self.assert_remote_timestamp('baz', 8000)
        self.assert_contents('baz', b'just syncing some stuff')

        self.sync_clients()

    def test_three_way_sync(self):
        self.create_local_client()
        self.create_local_client()
        self.create_local_client()

        set_local_contents(self.clients[0], 'foo', timestamp=1000)
        set_local_contents(self.clients[1], 'bar', timestamp=2000, data='red')
        set_local_contents(self.clients[2], 'baz', timestamp=3000)

        self.sync_clients()

        self.assert_local_keys(['foo', 'bar', 'baz'])
        self.assert_contents('bar', b'red')
        self.assert_remote_timestamp('foo', 1000)
        self.assert_remote_timestamp('bar', 2000)
        self.assert_remote_timestamp('baz', 3000)

        set_local_contents(self.clients[1], 'bar', timestamp=8000, data='green')
        self.sync_clients()

        self.assert_local_keys(['foo', 'bar', 'baz'])
        self.assert_contents('bar', b'green')
        self.assert_remote_timestamp('foo', 1000)
        self.assert_remote_timestamp('bar', 8000)
        self.assert_remote_timestamp('baz', 3000)

        delete_local(self.clients[2], 'foo')
        self.sync_clients()

        self.assert_file_existence(['foo'], False)
        self.assert_local_keys(['bar', 'baz'])

        self.sync_clients()


class TestMove(object):
    def test_correct_behaviour(self, local_client, s3_client):
        set_s3_contents(s3_client, 'art.txt', data='swirly abstract objects')
        sync.move(
            to_client=local_client,
            from_client=s3_client,
            key='art.txt',
            timestamp=6000,
        )

        assert get_local_contents(local_client, 'art.txt') == 'swirly abstract objects'
        assert local_client.get_remote_timestamp('art.txt') == 6000
