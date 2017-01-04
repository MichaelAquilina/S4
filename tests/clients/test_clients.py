# -*- coding: utf-8 -*-

import datetime

from s3backup.clients import SyncAction, SyncObject


class TestSyncAction(object):
    def test_repr(self):
        action = SyncAction(SyncAction.UPDATE, 1000)
        assert repr(action) == 'SyncAction<UPDATE, {}>'.format(
            datetime.datetime.utcfromtimestamp(1000)
        )


class TestSyncObject(object):
    def test_repr(self):
        dev_null = open('/dev/null', 'r')
        sync_object = SyncObject(dev_null, 312313)
        assert sync_object.timestamp == 312313
        assert sync_object.fp == dev_null
        assert repr(sync_object) == 'SyncObject<{}, 312313>'.format(dev_null)
