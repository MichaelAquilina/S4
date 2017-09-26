# -*- coding: utf-8 -*-

import datetime

import pytest

from s4.clients import SyncClient, SyncObject, SyncState, get_sync_state


class TestSyncState(object):
    def test_repr(self):
        action = SyncState(SyncState.UPDATED, 1000, 53434)
        assert repr(action) == 'SyncState<UPDATED, local={}, remote={}>'.format(
            datetime.datetime.utcfromtimestamp(1000),
            datetime.datetime.utcfromtimestamp(53434)
        )

    def test_get_local_datetime_missing(self):
        action = SyncState(SyncState.UPDATED, None, None)
        assert action.get_local_datetime() is None

    def test_get_remote_datetime_missing(self):
        action = SyncState(SyncState.CREATED, None, None)
        assert action.get_remote_datetime() is None

    def test_equality_with_self(self):
        action = SyncState(SyncState.UPDATED, 1000, 53434)
        assert action == action

    def test_non_equality(self):
        assert SyncState(SyncState.UPDATED, 10, 30) != SyncState(SyncState.DELETED, 10, 30)
        assert SyncState(SyncState.DELETED, 20, 20) != SyncState(SyncState.DELETED, 40, 20)
        assert SyncState(SyncState.CONFLICT, 20, 20) != SyncState(SyncState.CONFLICT, 20, 40)

    def test_equality_with_wrong_type(self):
        action = SyncState(SyncState.DELETED, 1000, 3000)
        assert action != 10
        assert action != "hello"


class TestSyncClient(object):
    def test_lock(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.lock()

    def test_unlock(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.unlock()

    def test_get_client_name(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_client_name()

    def test_get_uri(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_uri()

    def test_put(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.put("something", None)

    def test_get(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get("something")

    def test_delete(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.delete("something")

    def test_get_local_keys(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_local_keys()

    def test_get_real_local_timestamp(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_real_local_timestamp("something")

    def test_get_index_keys(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_index_keys()

    def test_get_index_local_timestamp(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_index_local_timestamp("something")

    def test_set_index_local_timestamp(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.set_index_local_timestamp("something", None)

    def test_get_remote_timestamp(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_remote_timestamp("something")

    def test_get_all_remote_timestamps(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_all_remote_timestamps()

    def test_get_all_index_local_timestamps(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_all_index_local_timestamps()

    def test_get_all_real_local_timestamps(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.get_all_real_local_timestamps()

    def test_flush_index(self):
        client = SyncClient()
        with pytest.raises(NotImplementedError):
            client.flush_index()


class TestSyncObject(object):
    def test_repr(self):
        dev_null = open('/dev/null', 'r')
        sync_object = SyncObject(
            fp=dev_null,
            total_size=4096,
            timestamp=312313,
        )
        assert sync_object.fp == dev_null
        assert sync_object.total_size == 4096
        assert sync_object.timestamp == 312313

        expected_repr = 'SyncObject<{}, 4096, 312313>'.format(dev_null)
        assert repr(sync_object) == expected_repr


class TestGetSyncState(object):
    def test_does_not_exist(self):
        assert get_sync_state(None, None, None) == SyncState(SyncState.DOESNOTEXIST, None, None)

    def test_no_changes(self):
        actual_state = get_sync_state(
            index_local=8130,
            real_local=8130,
            remote=8130,
        )
        expected_state = SyncState(SyncState.NOCHANGES, 8130, 8130)
        assert actual_state == expected_state

    def test_deleted(self):
        actual_state = get_sync_state(
            index_local=90000,
            real_local=None,
            remote=90000,
        )
        expected_state = SyncState(SyncState.DELETED, None, 90000)
        assert actual_state == expected_state

    def test_already_deleted(self):
        actual_state = get_sync_state(
            index_local=None,
            real_local=None,
            remote=77777,
        )
        expected_state = SyncState(SyncState.DELETED, None, 77777)
        assert actual_state == expected_state

    def test_updated(self):
        actual_state = get_sync_state(
            index_local=5000,
            real_local=6000,
            remote=5000,
        )
        expected_state = SyncState(SyncState.UPDATED, 6000, 5000)
        assert actual_state == expected_state

    def test_created(self):
        actual_state = get_sync_state(
            index_local=None,
            real_local=6000,
            remote=None,
        )
        expected_state = SyncState(SyncState.CREATED, 6000, None)
        assert actual_state == expected_state

    def test_conflict(self):
        actual_state = get_sync_state(
            index_local=6000,
            real_local=5000,
            remote=6000,
        )
        expected_state = SyncState(SyncState.CONFLICT, 6000, 6000)
        assert actual_state == expected_state

    def test_ignores_floating_precision(self):
        actual_state = get_sync_state(
            index_local=8000.80,
            real_local=8000.32,
            remote=6000,
        )
        expected_state = SyncState(SyncState.NOCHANGES, 8000, 6000)
        assert actual_state == expected_state
