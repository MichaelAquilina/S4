# -*- coding: utf-8 -*-

import json
import os

from s3backup import sync
from s3backup.clients import SyncState
from utils import set_s3_contents, set_local_contents, set_local_index, set_s3_index


def get_local_contents(local_client, key):
    path = os.path.join(local_client.path, key)
    with open(path, 'r') as fp:
        data = fp.read()
    return data


def delete_local(client, key):
    os.remove(os.path.join(client.path, key))


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
        deferred_calls, unhandled_events = sync.get_sync_actions(local_client, s3_client)
        assert deferred_calls == {}
        assert unhandled_events == {}


def assert_contents(clients, key, data=None, timestamp=None):
    for client in clients:
        sync_object = client.get(key)
        if data is not None:
            assert sync_object.fp.read() == data
        if timestamp is not None:
            assert sync_object.timestamp == timestamp


def assert_remote_timestamp(clients, key, expected_timestamp):
    for client in clients:
        assert client.get_remote_timestamp(key) == expected_timestamp


def assert_local_keys(clients, expected_keys):
    for client in clients:
        assert sorted(client.get_local_keys()) == sorted(expected_keys)


def assert_existence(clients, keys, exists):
    for client in clients:
        for key in keys:
            assert (client.get(key) is not None) == exists


class TestIntegrations(object):
    def test_local_with_s3(self, local_client, s3_client):
        set_s3_contents(s3_client, 'colors/cream', 9999, '#ddeeff')

        set_local_contents(local_client, 'colors/red', 5000, '#ff0000')
        set_local_contents(local_client, 'colors/green', 3000, '#00ff00')
        set_local_contents(local_client, 'colors/blue', 2000, '#0000ff')

        sync.sync(local_client, s3_client)

        clients = [local_client, s3_client]
        expected_keys = [
            'colors/red',
            'colors/green',
            'colors/blue',
            'colors/cream'
        ]
        assert_local_keys(clients, expected_keys)
        assert_contents(clients, 'colors/red', b'#ff0000')
        assert_contents(clients, 'colors/green', b'#00ff00')
        assert_contents(clients, 'colors/blue', b'#0000ff')
        assert_contents(clients, 'colors/cream', b'#ddeeff')
        assert_remote_timestamp(clients, 'colors/red', 5000)
        assert_remote_timestamp(clients, 'colors/green', 3000)
        assert_remote_timestamp(clients, 'colors/blue', 2000)
        assert_remote_timestamp(clients, 'colors/cream', 9999)

        delete_local(local_client, 'colors/red')

        sync.sync(local_client, s3_client)
        expected_keys = [
            'colors/green',
            'colors/blue',
            'colors/cream'
        ]
        assert_local_keys(clients, expected_keys)

    def test_fresh_sync(self, local_client, s3_client):
        set_local_contents(local_client, 'foo', timestamp=1000)
        set_local_contents(local_client, 'bar', timestamp=2000)
        set_s3_contents(s3_client, 'baz', timestamp=3000, data='what is up?')

        sync.sync(local_client, s3_client)

        clients = [local_client, s3_client]
        assert_local_keys(clients, ['foo', 'bar', 'baz'])
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'bar', 2000)
        assert_remote_timestamp(clients, 'baz', 3000)
        assert_existence(clients, ['foo', 'bar', 'baz'], True)
        assert_contents(clients, 'baz', b'what is up?')

        delete_local(local_client, 'foo')
        set_local_contents(local_client, 'test', timestamp=5000)
        set_s3_contents(s3_client, 'hello', timestamp=6000)
        set_s3_contents(s3_client, 'baz', timestamp=8000, data='just syncing some stuff')

        sync.sync(local_client, s3_client)

        assert_existence(clients, ['foo'], False)
        assert_local_keys(clients, ['test', 'bar', 'baz', 'hello'])
        assert_remote_timestamp(clients, 'bar', 2000)
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'test', 5000)
        assert_remote_timestamp(clients, 'hello', 6000)
        assert_remote_timestamp(clients, 'baz', 8000)
        assert_contents(clients, 'baz', b'just syncing some stuff')

        sync.sync(local_client, s3_client)

    def test_three_way_sync(self, local_client, s3_client, local_client_2):
        set_local_contents(local_client, 'foo', timestamp=1000)
        set_s3_contents(s3_client, 'bar', timestamp=2000, data='red')
        set_local_contents(local_client_2, 'baz', timestamp=3000)

        sync.sync(local_client, s3_client)
        sync.sync(local_client_2, s3_client)
        sync.sync(local_client, local_client_2)

        clients = [local_client, s3_client, local_client_2]

        assert_local_keys(clients, ['foo', 'bar', 'baz'])
        assert_contents(clients, 'bar', b'red')
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'bar', 2000)
        assert_remote_timestamp(clients, 'baz', 3000)

        set_s3_contents(s3_client, 'bar', timestamp=8000, data='green')

        sync.sync(local_client, s3_client)
        sync.sync(local_client_2, s3_client)
        sync.sync(local_client, local_client_2)

        assert_local_keys(clients, ['foo', 'bar', 'baz'])
        assert_contents(clients, 'bar', b'green')
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'bar', 8000)
        assert_remote_timestamp(clients, 'baz', 3000)

        delete_local(local_client_2, 'foo')

        sync.sync(local_client, s3_client)
        sync.sync(local_client_2, s3_client)
        sync.sync(local_client, local_client_2)

        assert_existence(clients, ['foo'], False)
        assert_local_keys(clients, ['bar', 'baz'])
        assert_remote_timestamp(clients, 'foo', 1000)

        sync.sync(local_client, s3_client)
        sync.sync(local_client_2, s3_client)
        sync.sync(local_client, local_client_2)


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
