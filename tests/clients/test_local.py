# -*- coding: utf-8 -*-

import os
import json
import shutil
import tempfile

from s3backup.clients import local


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
        assert list(local.traverse(self.target_folder)) == []

    def test_correct_output(self):
        touch(os.path.join(self.target_folder, 'foo'))
        touch(os.path.join(self.target_folder, 'bar.md'))
        touch(os.path.join(self.target_folder, 'baz', 'bar'))
        touch(os.path.join(self.target_folder, '.index'))

        actual_output = list(local.traverse(self.target_folder))
        expected_output = ['foo', 'bar.md', 'baz/bar']
        assert sorted(actual_output) == sorted(expected_output)


class TestLocalSyncClient(object):
    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    def test_index_path(self):
        client = local.LocalSyncClient(self.target_folder)
        assert client.index_path() == os.path.join(self.target_folder, '.index')

    def test_get_index_state(self):
        client = local.LocalSyncClient(self.target_folder)
        with open(client.index_path(), 'w') as fp:
            json.dump({
                'foo': {'timestamp': 4000},
                'bar/baz.txt': {'timestamp': 5000},
            }, fp)

        actual_output = client.get_index_state()
        expected_output = {
            'foo': dict(timestamp=4000),
            'bar/baz.txt': dict(timestamp=5000),
        }
        assert actual_output == expected_output

    def test_get_current_state(self):
        client = local.LocalSyncClient(self.target_folder)
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        actual_output = client.get_current_state()
        expected_output = {
            'foo': dict(timestamp=13371337),
            'bar': dict(timestamp=50032003),
        }
        assert sorted(expected_output) == sorted(actual_output)

    def test_update_index(self):
        client = local.LocalSyncClient(self.target_folder)
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        client.update_index()
        assert client.get_index_state() == client.get_current_state()
