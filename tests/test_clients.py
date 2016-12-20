# -*- coding: utf-8 -*-

import os
import json
import shutil
import tempfile

from s3backup.clients import SyncAction, compare, traverse, FileEntry, LocalSyncClient


def touch(path, mtime=None):
    if mtime is None:
        times = None
    else:
        times = (mtime, mtime)

    parent = os.path.dirname(path)
    if not os.path.exists(parent):
        os.makedirs(parent)

    with open(path, 'w'):
        os.utime(path, times)


class TestTraverse(object):
    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    def test_empty_folder(self):
        assert list(traverse(self.target_folder)) == []

    def test_correct_output(self):
        touch(os.path.join(self.target_folder, 'foo'))
        touch(os.path.join(self.target_folder, 'bar.md'))
        touch(os.path.join(self.target_folder, 'baz', 'bar'))
        touch(os.path.join(self.target_folder, '.index'))

        actual_output = list(traverse(self.target_folder))
        expected_output = ['foo', 'bar.md', 'baz/bar']
        assert sorted(actual_output) == sorted(expected_output)


class TestFileEntry(object):
    def test_repr(self):
        entry = FileEntry('foo/bar/baz.txt', 231230.4)
        assert repr(entry) == 'FileEntry(foo/bar/baz.txt, 231230.4)'


class TestLocalSyncClient(object):
    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    def test_index_path(self):
        client = LocalSyncClient(self.target_folder)
        assert client.index_path() == os.path.join(self.target_folder, '.index')

    def test_get_index_state(self):
        client = LocalSyncClient(self.target_folder)
        with open(client.index_path(), 'w') as fp:
            json.dump({
                'foo': {'timestamp': 4000},
                'bar/baz.txt': {'timestamp': 5000},
            }, fp)

        actual_output = client.get_index_state()
        expected_output = {
            'foo': FileEntry('foo', 4000),
            'bar/baz.txt': FileEntry('bar/baz.txt', 5000),
        }
        assert actual_output == expected_output

    def test_get_current_state(self):
        client = LocalSyncClient(self.target_folder)
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        actual_output = client.get_current_state()
        expected_output = {
            'foo': FileEntry('foo', 13371337),
            'bar': FileEntry('bar', 50032003),
        }
        assert sorted(expected_output) == sorted(actual_output)

    def test_update_index(self):
        client = LocalSyncClient(self.target_folder)
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        client.update_index()
        assert client.get_index_state() == client.get_current_state()


class TestCompare(object):
    def test_both_empty(self):
        assert list(compare({}, {})) == []

    def test_empty_current(self):
        current = {}
        previous = {
            'orange': FileEntry('orange', 99999),
            'apple': FileEntry('apple', 88888),
        }

        actual_output = list(compare(current, previous))
        expected_output = [
            (SyncAction.DELETE, 'apple'),
            (SyncAction.DELETE, 'orange'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_empty_previous(self):
        current = {
            'foo': FileEntry('foo', 400123),
            'bar': FileEntry('bar', 23231),
        }
        previous = {}

        actual_output = list(compare(current, previous))
        expected_output = [
            (SyncAction.CREATE, 'foo'),
            (SyncAction.CREATE, 'bar'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_current(self):
        current = {
            'red': FileEntry('red', 1234567),
        }
        previous = {
            'red': FileEntry('red', 1000000),
        }

        actual_output = list(compare(current, previous))
        expected_output = [
            (SyncAction.UPDATE, 'red'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_previous(self):
        current = {
            'monkey': FileEntry('monkey', 8000),
        }
        previous = {
            'monkey': FileEntry('monkey', 1000000),
        }

        actual_output = list(compare(current, previous))
        expected_output = [
            (SyncAction.CONFLICT, 'monkey'),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_mixed(self):
        current = {
            'monkey': FileEntry('monkey', 8000),
            'elephant': FileEntry('elephant', 3232323),
            'dog': FileEntry('dog', 23233232323),
        }
        previous = {
            'monkey': FileEntry('monkey', 1000000),
            'snake': FileEntry('snake', 232323),
            'dog': FileEntry('dog', 2333)
        }

        actual_output = list(compare(current, previous))
        expected_output = [
            (SyncAction.CREATE, 'elephant'),
            (SyncAction.CONFLICT, 'monkey'),
            (SyncAction.DELETE, 'snake'),
            (SyncAction.UPDATE, 'dog'),
        ]
        assert sorted(actual_output) == sorted(expected_output)
