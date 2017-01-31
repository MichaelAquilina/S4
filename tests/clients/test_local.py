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


def get_file_data(client, key):
    with open(os.path.join(client.path, key), 'rb') as fp:
        return fp.read()

def set_file_data(client, key, data):
    with open(os.path.join(client.path, key), 'wb') as fp:
        fp.write(data)

def set_index(client, data):
    with open(client.index_path(), 'w') as fp:
        json.dump(data, fp)
    client.reload_index()


class TestLocalSyncClient(object):
    def test_put_new(self, local_client):
        data = b'hi'
        local_client.put(
            key='hello_world.txt',
            sync_object=SyncObject(io.BytesIO(data), len(data), 20000)
        )

        assert local_client.index['hello_world.txt']['remote_timestamp'] == 20000
        assert get_file_data(local_client, 'hello_world.txt') == data

    def test_put_existing(self, local_client):
        set_index(local_client, {
            'doge.txt': {'local_timestamp': 1111111}
        })

        data = b'canis lupus familiaris'
        local_client.put(
            key='doge.txt',
            sync_object=SyncObject(io.BytesIO(data), len(data), 20000)
        )

        assert local_client.index['doge.txt']['remote_timestamp'] == 20000
        assert local_client.index['doge.txt']['local_timestamp'] == 1111111

        assert get_file_data(local_client, 'doge.txt') == data

    def test_get_existing(self, local_client):
        data = b'blue green yellow'
        set_file_data(local_client, 'whatup.md', data)
        sync_object = local_client.get('whatup.md')
        assert sync_object.fp.read() == data
        assert sync_object.total_size == len(data)

    def test_get_non_existant(self, local_client):
        assert local_client.get('idontexist.md') is None

    def test_delete_existing(self, local_client):
        target_file = os.path.join(local_client.path, 'foo')
        touch(os.path.join(local_client.path, 'foo'), 222222)

        assert os.path.exists(target_file) is True
        assert local_client.delete('foo') is True
        assert os.path.exists(target_file) is False

    def test_delete_non_existant(self, local_client):
        assert local_client.delete('idontexist.txt') is False

    def test_index_path(self):
        client = local.LocalSyncClient('/hello/from/the/magic/tavern')
        assert client.index_path() == '/hello/from/the/magic/tavern/.index'

    def test_load_index(self, local_client):
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
        set_index(local_client, data)

        assert local_client.index == data

    def test_get_local_keys(self, local_client):
        set_index(local_client, {})  # .index file should not come up in results
        touch(os.path.join(local_client.path, '.bashrc'))
        touch(os.path.join(local_client.path, 'foo'))
        touch(os.path.join(local_client.path, 'bar'))

        actual_output = local_client.get_local_keys()
        expected_output = ['foo', 'bar', '.bashrc']
        assert sorted(expected_output) == sorted(actual_output)

    def test_get_index_keys(self, local_client):
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
        set_index(local_client, data)

        actual_output = local_client.get_index_keys()
        expected_output = ['foo', 'bar/baz.txt']
        assert sorted(list(actual_output)) == sorted(expected_output)

    def test_all_keys(self, local_client):
        touch(os.path.join(local_client.path, '.index'))
        touch(os.path.join(local_client.path, 'foo'))
        touch(os.path.join(local_client.path, 'bar/boo.md'))
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
        set_index(local_client, data)

        actual_output = local_client.get_all_keys()
        expected_output = ['foo', 'bar/boo.md', 'bar/baz.txt']
        assert sorted(actual_output) == sorted(expected_output)

    def test_update_index_empty(self, local_client):
        touch(os.path.join(local_client.path, 'foo'), 13371337)
        touch(os.path.join(local_client.path, 'bar'), 50032003)

        local_client.update_index()
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
        assert local_client.index == expected_output

    def test_update_index_non_empty(self, local_client):
        touch(os.path.join(local_client.path, 'foo'), 13371337)
        touch(os.path.join(local_client.path, 'bar'), 50032003)

        set_index(local_client, {
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

        local_client.update_index()
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
        assert local_client.index == expected_output

    def test_get_real_local_timestamp(self, local_client):
        touch(os.path.join(local_client.path, 'atcg'), 2323230)

        assert local_client.get_real_local_timestamp('atcg') == 2323230
        assert local_client.get_real_local_timestamp('dontexist') is None

    def test_get_all_real_local_timestamps(self, local_client):
        touch(os.path.join(local_client.path, 'red'), 2323230)
        touch(os.path.join(local_client.path, 'blue'), 80808008)

        expected_output = {
            'red': 2323230,
            'blue': 80808008,
        }
        actual_output = local_client.get_all_real_local_timestamps()
        assert actual_output == expected_output

    def test_get_index_local_timestamp(self, local_client):
        set_index(local_client, {
            'foo': {
                'local_timestamp': 4000,
                'remote_timestamp': 32000,
            },
            'bar': {
                'local_timestamp': 9999999,
            }
        })
        assert local_client.get_index_local_timestamp('foo') == 4000
        assert local_client.get_index_local_timestamp('dontexist') is None
        assert local_client.get_index_local_timestamp('bar') == 9999999

        assert local_client.get_remote_timestamp('foo') == 32000
        assert local_client.get_remote_timestamp('dontexist') is None
        assert local_client.get_remote_timestamp('bar') is None

    def test_get_all_index_local_timestamps(self, local_client):
        set_index(local_client, {
            'frap': {
                'local_timestamp': 4000,
            },
            'brap': {
                'local_timestamp': 9999999,
            }
        })

        expected_output = {
            'frap': 4000,
            'brap': 9999999,
        }
        actual_output = local_client.get_all_index_local_timestamps()
        assert actual_output == expected_output

    def test_get_all_remote_timestamps(self, local_client):
        set_index(local_client, {
            'frap': {
                'remote_timestamp': 4000,
            },
            'brap': {
                'remote_timestamp': 9999999,
            }
        })

        expected_output = {
            'frap': 4000,
            'brap': 9999999,
        }
        actual_output = local_client.get_all_remote_timestamps()
        assert actual_output == expected_output
