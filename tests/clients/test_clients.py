# -*- coding: utf-8 -*-

import datetime

from s4.clients import get_sync_state, SyncState, SyncObject


class TestSyncState(object):
    def test_repr(self):
        action = SyncState(SyncState.UPDATED, 1000, 53434)
        assert repr(action) == 'SyncState<UPDATED, local={}, remote={}>'.format(
            datetime.datetime.utcfromtimestamp(1000),
            datetime.datetime.utcfromtimestamp(53434)
        )

    def test_equality_with_self(self):
        action = SyncState(SyncState.UPDATED, 1000, 53434)
        assert action == action

    def test_non_equality(self):
        assert SyncState(SyncState.UPDATED, 10, 30) != SyncState(SyncState.DELETED, 10, 30)
        assert SyncState(SyncState.DELETED, 20, 20) != SyncState(SyncState.DELETED, 40, 20)
        assert SyncState(SyncState.CONFLICT, 20, 20) != SyncState(SyncState.CONFLICT, 20, 40)


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
