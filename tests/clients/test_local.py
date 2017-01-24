# -*- coding: utf-8 -*-

import io
import json
import os
import shutil
import tempfile

from s3backup.clients import local, SyncState, SyncObject


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

    def test_non_existent_folder(self):
        assert list(local.traverse('/i/definetely/do/not/exist')) == []

    def test_empty_folder(self):
        assert list(local.traverse(self.target_folder)) == []

    def test_correct_output(self):
        touch(os.path.join(self.target_folder, 'baz', 'zoo'))
        touch(os.path.join(self.target_folder, 'foo'))
        touch(os.path.join(self.target_folder, 'bar.md'))
        touch(os.path.join(self.target_folder, 'baz', 'bar'))
        touch(os.path.join(self.target_folder, '.index'))
        touch(os.path.join(self.target_folder, 'garbage~'))
        touch(os.path.join(self.target_folder, 'saw/.index'))

        actual_output = list(local.traverse(
            self.target_folder,
            ignore_files={'.index', '.idontexist', '*~'}
        ))

        expected_output = ['bar.md', 'baz/bar', 'baz/zoo', 'foo']
        assert sorted(actual_output) == sorted(expected_output)


class TestLocalSyncClient(object):
    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()
        self.index_path = os.path.join(self.target_folder, '.index')

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    def test_repr(self):
        client = local.LocalSyncClient('/home/rick/timemachine/')
        assert repr(client) == 'LocalSyncClient</home/rick/timemachine/>'

    def set_index(self, data):
        with open(self.index_path, 'w') as fp:
            json.dump(data, fp)

    def get_file_data(self, key):
        with open(os.path.join(self.target_folder, key), 'rb') as fp:
            return fp.read()

    def set_file_data(self, key, data):
        with open(os.path.join(self.target_folder, key), 'wb') as fp:
            fp.write(data)

    def test_put_new(self):
        client = local.LocalSyncClient(self.target_folder)
        data = b'hi'
        client.put(
            key='hello_world.txt',
            sync_object=SyncObject(io.BytesIO(data), len(data), 20000)
        )

        assert client.index['hello_world.txt']['remote_timestamp'] == 20000
        assert self.get_file_data('hello_world.txt') == data

    def test_put_existing(self):
        self.set_index({
            'doge.txt': {'local_timestamp': 1111111}
        })

        data = b'canis lupus familiaris'
        client = local.LocalSyncClient(self.target_folder)
        client.put(
            key='doge.txt',
            sync_object=SyncObject(io.BytesIO(data), len(data), 20000)
        )

        assert client.index['doge.txt']['remote_timestamp'] == 20000
        assert client.index['doge.txt']['local_timestamp'] == 1111111

        assert self.get_file_data('doge.txt') == data

    def test_get_existing(self):
        client = local.LocalSyncClient(self.target_folder)

        data = b'blue green yellow'
        self.set_file_data('whatup.md', data)
        sync_object = client.get('whatup.md')
        assert sync_object.fp.read() == data
        assert sync_object.total_size == len(data)

    def test_get_non_existant(self):
        client = local.LocalSyncClient(self.target_folder)
        assert client.get('idontexist.md') is None

    def test_delete_existing(self):
        target_file = os.path.join(self.target_folder, 'foo')
        touch(os.path.join(self.target_folder, 'foo'), 222222)
        client = local.LocalSyncClient(self.target_folder)

        assert os.path.exists(target_file) is True
        assert client.delete('foo') is True
        assert os.path.exists(target_file) is False

    def test_delete_non_existant(self):
        client = local.LocalSyncClient(self.target_folder)
        assert client.delete('idontexist.txt') is False

    def test_index_path(self):
        client = local.LocalSyncClient(self.target_folder)
        assert client.index_path() == self.index_path

    def test_load_index(self):
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
        self.set_index(data)

        client = local.LocalSyncClient(self.target_folder)
        assert client.index == data

    def test_get_local_keys(self):
        self.set_index({})  # .index file should not come up in results
        touch(os.path.join(self.target_folder, '.bashrc'))
        touch(os.path.join(self.target_folder, 'foo'))
        touch(os.path.join(self.target_folder, 'bar'))

        client = local.LocalSyncClient(self.target_folder)
        actual_output = client.get_local_keys()
        expected_output = ['foo', 'bar', '.bashrc']
        assert sorted(expected_output) == sorted(actual_output)

    def test_get_index_keys(self):
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
        self.set_index(data)
        client = local.LocalSyncClient(self.target_folder)
        actual_output = client.get_index_keys()
        expected_output = ['foo', 'bar/baz.txt']
        assert sorted(list(actual_output)) == sorted(expected_output)

    def test_all_keys(self):
        touch(os.path.join(self.target_folder, '.index'))
        touch(os.path.join(self.target_folder, 'foo'))
        touch(os.path.join(self.target_folder, 'bar/boo.md'))
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
        self.set_index(data)
        client = local.LocalSyncClient(self.target_folder)
        actual_output = client.get_all_keys()
        expected_output = ['foo', 'bar/boo.md', 'bar/baz.txt']
        assert sorted(actual_output) == sorted(expected_output)

    def test_update_index_empty(self):
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        client = local.LocalSyncClient(self.target_folder)
        client.update_index()
        # remote timestamp should not be included since it does not exist
        expected_output = {
            'foo': {
                'local_timestamp': 13371337,
                'remote_timestamp': None,
            },
            'bar': {
                'local_timestamp': 50032003,
                'remote_timestamp': None,
            },
        }
        assert client.index == expected_output

    def test_update_index_non_empty(self):
        touch(os.path.join(self.target_folder, 'foo'), 13371337)
        touch(os.path.join(self.target_folder, 'bar'), 50032003)

        self.set_index({
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
        })

        client = local.LocalSyncClient(self.target_folder)
        client.update_index()
        expected_output = {
            'foo': {
                'local_timestamp': 13371337,
                'remote_timestamp': 4000,
            },
            'bar': {
                'local_timestamp': 50032003,
                'remote_timestamp': 5000,
            },
            'baz': {
                'local_timestamp': None,
                'remote_timestamp': 5000,
            },
        }
        assert client.index == expected_output

    def test_get_real_local_timestamp(self):
        touch(os.path.join(self.target_folder, 'atcg'), 2323230)

        client = local.LocalSyncClient(self.target_folder)
        assert client.get_real_local_timestamp('atcg') == 2323230
        assert client.get_real_local_timestamp('dontexist') is None

    def test_get_all_real_local_timestamps(self):
        touch(os.path.join(self.target_folder, 'red'), 2323230)
        touch(os.path.join(self.target_folder, 'blue'), 80808008)

        client = local.LocalSyncClient(self.target_folder)
        expected_output = {
            'red': 2323230,
            'blue': 80808008,
        }
        actual_output = client.get_all_real_local_timestamps()
        assert actual_output == expected_output

    def test_get_index_local_timestamp(self):
        self.set_index({
            'foo': {
                'local_timestamp': 4000,
                'remote_timestamp': 32000,
            },
            'bar': {
                'local_timestamp': 9999999,
            }
        })
        client = local.LocalSyncClient(self.target_folder)
        assert client.get_index_local_timestamp('foo') == 4000
        assert client.get_index_local_timestamp('dontexist') is None
        assert client.get_index_local_timestamp('bar') == 9999999

        assert client.get_remote_timestamp('foo') == 32000
        assert client.get_remote_timestamp('dontexist') is None
        assert client.get_remote_timestamp('bar') is None

    def test_get_all_index_local_timestamps(self):
        self.set_index({
            'frap': {
                'local_timestamp': 4000,
            },
            'brap': {
                'local_timestamp': 9999999,
            }
        })

        client = local.LocalSyncClient(self.target_folder)
        expected_output = {
            'frap': 4000,
            'brap': 9999999,
        }
        actual_output = client.get_all_index_local_timestamps()
        assert actual_output == expected_output

    def test_get_all_remote_timestamps(self):
        self.set_index({
            'frap': {
                'remote_timestamp': 4000,
            },
            'brap': {
                'remote_timestamp': 9999999,
            }
        })

        client = local.LocalSyncClient(self.target_folder)
        expected_output = {
            'frap': 4000,
            'brap': 9999999,
        }
        actual_output = client.get_all_remote_timestamps()
        assert actual_output == expected_output
