# -*- coding: utf-8 -*-
from s3backup import sync


class TestFileEntry(object):
    def test_repr(self):
        entry = sync.FileEntry('foo/bar/baz.txt', 231230.4)
        assert repr(entry) == 'FileEntry(foo/bar/baz.txt, 231230.4)'


class TestCompareStates(object):
    def test_both_empty(self):
        assert list(sync.compare_states({}, {})) == []

    def test_empty_current(self):
        current = {}
        previous = {
            'orange': sync.FileEntry('orange', 99999),
            'apple': sync.FileEntry('apple', 88888),
        }

        actual_output = list(sync.compare_states(current, previous))
        expected_output = [
            (sync.SyncAction.DELETE, 'apple'),
            (sync.SyncAction.DELETE, 'orange'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_empty_previous(self):
        current = {
            'foo': sync.FileEntry('foo', 400123),
            'bar': sync.FileEntry('bar', 23231),
        }
        previous = {}

        actual_output = list(sync.compare_states(current, previous))
        expected_output = [
            (sync.SyncAction.CREATE, 'foo'),
            (sync.SyncAction.CREATE, 'bar'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_current(self):
        current = {
            'red': sync.FileEntry('red', 1234567),
        }
        previous = {
            'red': sync.FileEntry('red', 1000000),
        }

        actual_output = list(sync.compare_states(current, previous))
        expected_output = [
            (sync.SyncAction.UPDATE, 'red'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_previous(self):
        current = {
            'monkey': sync.FileEntry('monkey', 8000),
        }
        previous = {
            'monkey': sync.FileEntry('monkey', 1000000),
        }

        actual_output = list(sync.compare_states(current, previous))
        expected_output = [
            (sync.SyncAction.CONFLICT, 'monkey'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_mixed(self):
        current = {
            'monkey': sync.FileEntry('monkey', 8000),
            'elephant': sync.FileEntry('elephant', 3232323),
            'dog': sync.FileEntry('dog', 23233232323),
        }
        previous = {
            'monkey': sync.FileEntry('monkey', 1000000),
            'snake': sync.FileEntry('snake', 232323),
            'dog': sync.FileEntry('dog', 2333)
        }

        actual_output = list(sync.compare_states(current, previous))
        expected_output = [
            (sync.SyncAction.CREATE, 'elephant'),
            (sync.SyncAction.CONFLICT, 'monkey'),
            (sync.SyncAction.DELETE, 'snake'),
            (sync.SyncAction.UPDATE, 'dog'),
        ]
        assert sorted(actual_output) == sorted(expected_output)
