# -*- coding: utf-8 -*-
import mock

import pytest

from s4 import sync
from s4.clients import SyncState, local, s3
from s4.sync import Resolution
from tests import utils


class TestGetStates(object):
    def test_empty_clients(self, s3_client, local_client):
        worker = sync.SyncWorker(s3_client, local_client)
        actual_output = list(worker.get_states())
        assert actual_output == []


class TestGetSyncStates(object):
    def test_empty(self, local_client, s3_client):
        assert sync.SyncWorker(local_client, s3_client).get_sync_states() == ({}, {})

    def test_correct_output(self, local_client, s3_client):
        utils.set_local_index(local_client, {
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
        utils.set_s3_index(s3_client, {
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

        utils.set_local_contents(local_client, 'history.txt', timestamp=5000)
        utils.set_s3_contents(s3_client, 'art.txt', timestamp=200000)
        utils.set_local_contents(local_client, 'english.txt', timestamp=90000)
        utils.set_s3_contents(s3_client, 'english.txt', timestamp=93000)
        utils.set_s3_contents(s3_client, 'chemistry.txt', timestamp=10000)
        utils.set_local_contents(local_client, 'physics.txt', timestamp=11000)
        utils.set_s3_contents(s3_client, 'physics.txt', timestamp=13000)
        utils.set_local_contents(local_client, 'maltese.txt', timestamp=7000)
        utils.set_s3_contents(s3_client, 'maltese.txt', timestamp=8000)

        worker = sync.SyncWorker(local_client, s3_client)
        resolutions, unhandled_events = worker.get_sync_states()
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
        expected_resolutions = {
            'maltese.txt': Resolution(
                Resolution.UPDATE, local_client, s3_client, 'maltese.txt', 8000
            ),
            'chemistry.txt': Resolution(
                Resolution.DELETE, s3_client, None, 'chemistry.txt', 9431
            ),
            'history.txt': Resolution(
                Resolution.CREATE, s3_client, local_client, 'history.txt', 5000
            ),
            'art.txt': Resolution(
                Resolution.CREATE, local_client, s3_client, 'art.txt', 200000
            ),
        }
        assert unhandled_events == expected_unhandled_events
        assert resolutions == expected_resolutions

    def test_nochanges_but_different_remote_timestamps(self, local_client, s3_client):
        utils.set_local_index(local_client, {
            'german.txt': {
                'local_timestamp': 4000,
                'remote_timestamp': 4000,
            }
        })
        utils.set_s3_index(s3_client, {
            'german.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            }
        })
        utils.set_local_contents(local_client, 'german.txt', timestamp=4000)
        utils.set_s3_contents(s3_client, 'german.txt', timestamp=6000)

        worker = sync.SyncWorker(local_client, s3_client)
        resolutions, unhandled_events = worker.get_sync_states()
        expected_resolutions = {
            'german.txt': Resolution(
                "UPDATE", local_client, s3_client, 'german.txt', 6000)
        }
        assert resolutions == expected_resolutions
        assert unhandled_events == {}

    def test_updated_but_different_remote_timestamp(self, local_client, s3_client):
        utils.set_local_index(local_client, {
            'biology.txt': {
                'local_timestamp': 4000,
                'remote_timestamp': 3000,
            }
        })
        utils.set_s3_index(s3_client, {
            'biology.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            }
        })
        utils.set_local_contents(local_client, 'biology.txt', timestamp=4500)
        utils.set_s3_contents(s3_client, 'biology.txt', timestamp=6000)

        worker = sync.SyncWorker(local_client, s3_client)
        resolutions, unhandled_events = worker.get_sync_states()
        expected_unhandled_events = {
            'biology.txt': (
                SyncState(SyncState.UPDATED, 4500, 3000), SyncState(SyncState.NOCHANGES, 6000, 6000)
            )
        }
        assert resolutions == {}
        assert unhandled_events == expected_unhandled_events

    def test_deleted_but_different_remote_timestamp(self, local_client, s3_client):
        utils.set_local_index(local_client, {
            'chemistry.txt': {
                'local_timestamp': 4000,
                'remote_timestamp': 3000,
            }
        })
        utils.set_s3_index(s3_client, {
            'chemistry.txt': {
                'local_timestamp': 6000,
                'remote_timestamp': 6000,
            }
        })
        utils.set_s3_contents(s3_client, 'chemistry.txt', timestamp=6000)

        worker = sync.SyncWorker(local_client, s3_client)
        resolutions, unhandled_events = worker.get_sync_states()
        expected_unhandled_events = {
            'chemistry.txt': (
                SyncState(SyncState.DELETED, None, 3000), SyncState(SyncState.NOCHANGES, 6000, 6000)
            )
        }
        assert resolutions == {}
        assert unhandled_events == expected_unhandled_events

    def test_deleted_doesnotexist(self, local_client, s3_client):
        utils.set_local_index(local_client, {
            'physics.txt': {
                'local_timestamp': 5000,
                'remote_timestamp': 4550,
            }
        })
        worker = sync.SyncWorker(local_client, s3_client)
        resolutions, unhandled_events = worker.get_sync_states()
        assert resolutions == {}
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


class TestSyncWorker(object):
    def test_repr(self):
        local_client = local.LocalSyncClient('/home/bobs/burgers')
        s3_client = s3.S3SyncClient(None, 'burgerbucket', 'foozie')
        worker = sync.SyncWorker(local_client, s3_client)
        assert repr(worker) == 'SyncWorker</home/bobs/burgers/, s3://burgerbucket/foozie/>'


def TestGetResolution(object):
    def test_unknown(self, local_client, s3_client):
        state = SyncState("unkonwn state", None, None)
        with pytest.raises(ValueError):
            sync.get_resolution("test", state, local_client, s3_client)


class TestIntegrations(object):
    def test_dry_run(self, local_client, s3_client):
        utils.set_s3_contents(s3_client, 'colors/cream', 9999, '#ddeeff')

        utils.set_local_contents(local_client, 'colors/red', 5000, '#ff0000')
        utils.set_local_contents(local_client, 'colors/green', 3000, '#00ff00')
        utils.set_local_contents(local_client, 'colors/blue', 2000, '#0000ff')

        local_client.put = mock.MagicMock()
        s3_client.put = mock.MagicMock()

        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync(dry_run=True)

        assert not local_client.put.called
        assert not s3_client.put.called

    def test_action_callback(self, local_client, s3_client):
        utils.set_s3_contents(s3_client, 'colors/cream', 9999)

        utils.set_local_contents(local_client, 'colors/green', 3000)
        utils.set_local_contents(local_client, 'colors/blue', 2000)

        callback_mock = mock.MagicMock()

        worker = sync.SyncWorker(
            local_client,
            s3_client,
            action_callback=callback_mock
        )
        worker.sync()

        assert callback_mock.call_count == 3
        assert callback_mock.call_args_list[0][0][0].key == 'colors/blue'
        assert callback_mock.call_args_list[1][0][0].key == 'colors/cream'
        assert callback_mock.call_args_list[2][0][0].key == 'colors/green'

    def test_local_with_s3(self, local_client, s3_client):
        utils.set_s3_contents(s3_client, 'colors/cream', 9999, '#ddeeff')

        utils.set_local_contents(local_client, 'colors/red', 5000, '#ff0000')
        utils.set_local_contents(local_client, 'colors/green', 3000, '#00ff00')
        utils.set_local_contents(local_client, 'colors/blue', 2000, '#0000ff')

        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync()

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

        utils.delete_local(local_client, 'colors/red')

        worker.sync()
        expected_keys = [
            'colors/green',
            'colors/blue',
            'colors/cream'
        ]
        assert_local_keys(clients, expected_keys)

    def test_fresh_sync(self, local_client, s3_client):
        utils.set_local_contents(local_client, 'foo', timestamp=1000)
        utils.set_local_contents(local_client, 'bar', timestamp=2000)
        utils.set_s3_contents(s3_client, 'baz', timestamp=3000, data='what is up?')

        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync()

        clients = [local_client, s3_client]
        assert_local_keys(clients, ['foo', 'bar', 'baz'])
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'bar', 2000)
        assert_remote_timestamp(clients, 'baz', 3000)
        assert_existence(clients, ['foo', 'bar', 'baz'], True)
        assert_contents(clients, 'baz', b'what is up?')

        utils.delete_local(local_client, 'foo')
        utils.set_local_contents(local_client, 'test', timestamp=5000)
        utils.set_s3_contents(s3_client, 'hello', timestamp=6000)
        utils.set_s3_contents(s3_client, 'baz', timestamp=8000, data='just syncing some stuff')

        worker.sync()

        assert_existence(clients, ['foo'], False)
        assert_local_keys(clients, ['test', 'bar', 'baz', 'hello'])
        assert_remote_timestamp(clients, 'bar', 2000)
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'test', 5000)
        assert_remote_timestamp(clients, 'hello', 6000)
        assert_remote_timestamp(clients, 'baz', 8000)
        assert_contents(clients, 'baz', b'just syncing some stuff')

        worker.sync()

    def test_three_way_sync(self, local_client, s3_client, local_client_2):
        utils.set_local_contents(local_client, 'foo', timestamp=1000)
        utils.set_s3_contents(s3_client, 'bar', timestamp=2000, data='red')
        utils.set_local_contents(local_client_2, 'baz', timestamp=3000)

        worker_1 = sync.SyncWorker(local_client, s3_client)
        worker_2 = sync.SyncWorker(local_client_2, s3_client)
        worker_3 = sync.SyncWorker(local_client, local_client_2)

        worker_1.sync()
        worker_2.sync()
        worker_3.sync()

        clients = [local_client, s3_client, local_client_2]

        assert_local_keys(clients, ['foo', 'bar', 'baz'])
        assert_contents(clients, 'bar', b'red')
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'bar', 2000)
        assert_remote_timestamp(clients, 'baz', 3000)

        utils.set_s3_contents(s3_client, 'bar', timestamp=8000, data='green')

        worker_1.sync()
        worker_2.sync()
        worker_3.sync()

        assert_local_keys(clients, ['foo', 'bar', 'baz'])
        assert_contents(clients, 'bar', b'green')
        assert_remote_timestamp(clients, 'foo', 1000)
        assert_remote_timestamp(clients, 'bar', 8000)
        assert_remote_timestamp(clients, 'baz', 3000)

        utils.delete_local(local_client_2, 'foo')

        worker_1.sync()
        worker_2.sync()
        worker_3.sync()

        assert_existence(clients, ['foo'], False)
        assert_local_keys(clients, ['bar', 'baz'])
        assert_remote_timestamp(clients, 'foo', 1000)

        worker_1.sync()
        worker_2.sync()
        worker_3.sync()

    def test_ignore_conflicts(self, local_client, s3_client):
        utils.set_local_contents(local_client, 'foo', timestamp=2000)
        utils.set_s3_contents(s3_client, 'foo', timestamp=3000)
        utils.set_s3_contents(s3_client, 'bar', timestamp=5600, data='usador')

        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync(conflict_choice='ignore')

        clients = [local_client, s3_client]
        assert_local_keys(clients, ['foo', 'bar'])
        assert_remote_timestamp(clients, 'foo', None)
        assert_remote_timestamp(clients, 'bar', 5600)
        assert_existence(clients, ['foo', 'bar'], True)
        assert_contents(clients, 'bar', b'usador')

        # Check that none of the files have been modified due to ignore flag
        assert local_client.get_real_local_timestamp('foo') == 2000
        assert s3_client.get_real_local_timestamp('foo') == 3000

    def test_conflict_choose_first_client(self, local_client, s3_client):
        utils.set_local_contents(local_client, 'foo', timestamp=2000, data='abc')
        utils.set_s3_contents(s3_client, 'foo', timestamp=3000)

        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync(conflict_choice='1')

        clients = [local_client, s3_client]
        assert_local_keys(clients, ['foo'])
        # Chooses first client
        assert_remote_timestamp(clients, 'foo', 2000)
        assert_contents(clients, 'foo', b'abc')

    def test_conflict_choose_second_client(self, local_client, s3_client):
        utils.set_local_contents(local_client, 'foo', timestamp=2000)
        utils.set_s3_contents(s3_client, 'foo', timestamp=3000, data='123')

        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync(conflict_choice='2')

        clients = [local_client, s3_client]
        assert_local_keys(clients, ['foo'])
        # Chooses second client
        assert_remote_timestamp(clients, 'foo', 3000)
        assert_contents(clients, 'foo', b'123')

    def test_previously_deleted_now_created(self, local_client, s3_client):
        utils.set_s3_index(s3_client, {
            'foo': {
                'local_timestamp': None,
                'remote_timestamp': 4000,
            }
        })
        utils.set_local_index(local_client, {
            'foo': {
                'local_timstamp': None,
                'remote_timestamp': 4000,
            }
        })
        utils.set_local_contents(local_client, 'foo', timestamp=7000)

        # Will create previously deleted file
        worker = sync.SyncWorker(local_client, s3_client)
        worker.sync()

        clients = [local_client, s3_client]
        assert_local_keys(clients, ['foo'])
        assert_remote_timestamp(clients, 'foo', 7000)

        # delete the file again and check that it is successful
        utils.delete_local(local_client, 'foo')

        worker.sync()
        assert_local_keys(clients, [])
        assert_remote_timestamp(clients, 'foo', 7000)


class TestRunResolutions(object):
    def test_empty(self, local_client, s3_client):
        worker = sync.SyncWorker(local_client, s3_client)
        assert worker.run_resolutions({}) == []

    def test_invalid_action(self, local_client, s3_client):
        worker = sync.SyncWorker(local_client, s3_client)
        with pytest.raises(ValueError):
            worker.run_resolutions({
                'foo': Resolution('UNKNOWN', None, None, None, None),
            })

    def test_correct_output(self, local_client, s3_client):
        def keyboard_interrupt():
            raise KeyboardInterrupt()

        clients = [local_client, s3_client]
        worker = sync.SyncWorker(local_client, s3_client)

        utils.set_local_contents(local_client, 'foo')
        utils.set_s3_contents(s3_client, 'baz', timestamp=2000, data='testing')
        success = worker.run_resolutions({
            'foo': Resolution(Resolution.DELETE, local_client, None, 'foo', 1000),
            'baz': Resolution(Resolution.CREATE, local_client, s3_client, 'baz', 20),
        })
        assert sorted(success) == sorted(['foo', 'baz'])
        assert_local_keys(clients, ['baz'])


class TestMoveClient(object):
    def test_correct_behaviour(self, local_client, s3_client):
        utils.set_s3_contents(s3_client, 'art.txt', data='swirly abstract objects')

        worker = sync.SyncWorker(local_client, s3_client)

        resolution = Resolution(
            action=Resolution.CREATE,
            to_client=local_client,
            from_client=s3_client,
            key='art.txt',
            timestamp=6000,
        )
        worker.move_client(resolution)

        assert utils.get_local_contents(local_client, 'art.txt') == b'swirly abstract objects'
        assert local_client.get_remote_timestamp('art.txt') == 6000
