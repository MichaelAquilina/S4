# -*- coding: utf-8 -*-

import io
import json
import os
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
        touch(os.path.join(self.target_folder, 'saw/.index'))

        actual_output = list(local.traverse(
            self.target_folder,
            ignore_files={'.index', '.idontexist'}
        ))

        # TODO: This should be auto sorted based on the search mechanism used
        expected_output = ['foo', 'bar.md', 'baz/bar']
        assert sorted(actual_output) == sorted(expected_output)


class TestLocalSyncClient(object):
    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()
        self.index_path = os.path.join(self.target_folder, '.index')

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    def test_put(self):
        client = local.LocalSyncClient(self.target_folder)
        client.put('hello_world.txt', io.BytesIO(b'hi'), 20000)

        assert client.index['hello_world.txt']['remote_timestamp'] == 20000
        with open(os.path.join(self.target_folder, 'hello_world.txt'), 'rb') as fp:
            data = fp.read()
        assert data == b'hi'

    def test_get(self):
        client = local.LocalSyncClient(self.target_folder)
        with open(os.path.join(self.target_folder, 'whatup.md'), 'wb') as fp:
            fp.write(b'blue green yellow')
        fp = client.get('whatup.md')
        assert fp.read() == b'blue green yellow'

    def test_delete(self):
        target_file = os.path.join(self.target_folder, 'foo')
        touch(os.path.join(self.target_folder, 'foo'), 222222)
        client = local.LocalSyncClient(self.target_folder)

        assert os.path.exists(target_file) is True
        client.delete('foo')
        assert os.path.exists(target_file) is False

    def test_index_path(self):
        client = local.LocalSyncClient(self.target_folder)
        assert client.index_path() == self.index_path

    def test_get_index_state(self):
        data = {
            'foo': {
                'local_timestamp': 4000,
                'remote_timestamp': 4000,
            },
            'bar/baz.txt': {
                'local_timestamp': 5000,
                'remote_timestamp': 5000,
            },
        }
        with open(self.index_path, 'w') as fp:
            json.dump(data, fp)

        client = local.LocalSyncClient(self.target_folder)
        actual_output = client.get_index_state()
        assert actual_output == data

    def test_get_current_state(self):
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        client = local.LocalSyncClient(self.target_folder)
        actual_output = client.get_current_state()
        expected_output = {
            'foo': {'local_timestamp': 13371337},
            'bar': {'local_timestamp': 50032003},
        }
        assert expected_output == actual_output

    def test_update_index_empty(self):
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        client = local.LocalSyncClient(self.target_folder)
        client.update_index()
        actual_output = client.get_index_state()
        # remote timestamp should not be included since it does not exist
        expected_output = {
            'foo': {
                'local_timestamp': 13371337,
            },
            'bar': {
                'local_timestamp': 50032003,
            },
        }
        assert actual_output == expected_output

    def test_update_index_non_empty(self):
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        with open(self.index_path, 'w') as fp:
            json.dump({
                'foo': {
                    'local_timestamp': 4000,
                    'remote_timestamp': 4000,
                },
                'bar': {
                    'local_timestamp': 5000,
                    'remote_timestamp': 5000,
                },
                'baz': {
                    'local_timestamp': 5000,
                    'remote_timestamp': 5000,
                }
            }, fp)

        client = local.LocalSyncClient(self.target_folder)
        client.update_index()
        actual_output = client.get_index_state()
        expected_output = {
            'foo': {
                'local_timestamp': 13371337,
                'remote_timestamp': 4000,
            },
            'bar': {
                'local_timestamp': 50032003,
                'remote_timestamp': 5000,
            },
        }
        assert actual_output == expected_output
