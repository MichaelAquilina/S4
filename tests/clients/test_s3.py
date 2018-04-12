# -*- coding: utf-8 -*-
import datetime
import io
import os

import boto3

from botocore.exceptions import ClientError

import freezegun

import mock

from moto import mock_s3

import pytest

from s4.clients import SyncObject, s3
from s4.utils import to_timestamp

from tests import utils


class TestParseS3URI(object):
    def test_empty(self):
        assert s3.parse_s3_uri('') is None

    def test_incorrect_format(self):
        assert s3.parse_s3_uri('nos3infront/some/path') is None
        assert s3.parse_s3_uri(':/232red+-32') is None

    def test_no_path(self):
        assert s3.parse_s3_uri('s3://bucket') is None

    def test_correct_output(self):
        actual_output = s3.parse_s3_uri('s3://fruit.bowl/apples/and/oranges/')
        assert actual_output.bucket == 'fruit.bowl'
        assert actual_output.key == 'apples/and/oranges/'


class TestIsIgnoredFile(object):
    def test_empty(self):
        assert s3.is_ignored_key('foo/bar/baz', []) is False

    def test_subdirectory(self):
        assert s3.is_ignored_key('foo/.git/baz', ['.git']) is True

    def test_file(self):
        assert s3.is_ignored_key('foo/ignoreme', ['ignore*']) is True


class TestS3SyncClient(object):
    def test_get_client_name(self, s3_client):
        assert s3_client.get_client_name() == 's3'

    def test_repr(self):
        boto_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )

        client = s3.S3SyncClient(boto_client, 'testbucket', 'foo/bar')
        assert repr(client) == 'S3SyncClient<testbucket, foo/bar>'

    @mock_s3
    def test_get_uri(self):
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        assert client.get_uri() == 's3://testbucket/foo/bar/'
        assert client.get_uri('apples.txt') == 's3://testbucket/foo/bar/apples.txt'

    @mock_s3
    def test_index_path(self):
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        assert client.index_path() == 'foo/bar/.index'

    def test_put(self, s3_client):
        # given
        data = b'munchkin'

        # when
        input_object = SyncObject(io.BytesIO(data), len(data), 4000)
        s3_client.put('something/boardgame.rst', input_object)

        # then
        resp = s3_client.boto.get_object(
            Bucket=s3_client.bucket,
            Key=os.path.join(s3_client.prefix, 'something/boardgame.rst'),
        )
        assert resp['Body'].read() == data
        assert s3_client.get_remote_timestamp('something/boardgame.rst') == 4000

    def test_put_callback(self, s3_client):
        # given
        data = b'eygon of carim'
        mock_callback = mock.MagicMock()

        # when
        input_object = SyncObject(io.BytesIO(data), len(data), 4000)
        s3_client.put(
            'dark/souls.rst',
            input_object,
            callback=mock_callback,
        )

        assert mock_callback.call_count == 1
        mock_callback.assert_called_with(len(data))

    def test_get(self, s3_client):
        # given
        data = b'#000000'
        frozen_time = datetime.datetime(2016, 10, 23, 10, 30, tzinfo=datetime.timezone.utc)
        with freezegun.freeze_time(frozen_time):
            s3_client.boto.put_object(
                Bucket=s3_client.bucket,
                Key=os.path.join(s3_client.prefix, 'black.color'),
                Body=data,
            )

        # when
        output_object = s3_client.get('black.color')

        # then
        assert output_object.fp.read() == data
        assert output_object.total_size == len(data)
        assert output_object.timestamp == to_timestamp(frozen_time)

    def test_get_non_existant(self, s3_client):
        assert s3_client.get('idontexist.md') is None

    def test_put_get(self, s3_client):
        # given
        data = b'woooooooooshhh'
        input_object = SyncObject(io.BytesIO(data), len(data), 4000)
        frozen_time = datetime.datetime(2016, 10, 23, 10, 30, tzinfo=datetime.timezone.utc)

        # when
        with freezegun.freeze_time(frozen_time):
            s3_client.put('something/woosh.gif', input_object)

        # then
        output_object = s3_client.get('something/woosh.gif')
        assert output_object.fp.read() == data
        assert output_object.timestamp == to_timestamp(frozen_time)

    def test_delete(self, s3_client):
        # given
        s3_client.boto.put_object(
            Bucket=s3_client.bucket,
            Key=os.path.join(s3_client.prefix, 'war.png'),
            Body='bang',
        )

        # when
        assert s3_client.delete('war.png') is True

        # then
        assert s3_client.get('war.png') is None
        with pytest.raises(ClientError) as exc:
            s3_client.boto.head_object(
                Bucket=s3_client.bucket,
                Key=os.path.join(s3_client.prefix, 'war.png')
            )
        assert exc.value.response['Error']['Code'] == '404'

    def test_delete_non_existant(self, s3_client):
        assert s3_client.delete('idontexist.png') is False

    def test_get_local_keys(self, s3_client):
        # given
        utils.set_s3_contents(s3_client, 'war.png')
        utils.set_s3_contents(s3_client, 'this/is/fine')
        utils.set_s3_contents(s3_client, '.index')
        utils.write_s3(
            s3_client.boto,
            s3_client.bucket,
            os.path.join('iamsomethingelse', 'ishouldnotshow.txt'),
        )

        # when
        actual_output = s3_client.get_local_keys()

        # then
        expected_output = ['war.png', 'this/is/fine']
        assert sorted(actual_output) == sorted(expected_output)

    def test_get_index_keys(self, s3_client):
        utils.set_s3_index(s3_client, {
            'cow': {
                'local_timestamp': 4000,
                'remote_timestamp': 3000,
            },
            'chicken': {
                'local_timestamp': 4000,
                'remote_timestamp': 3000,
            }
        })
        assert sorted(s3_client.get_index_keys()) == sorted(['cow', 'chicken'])

    @pytest.mark.parametrize('compression', [None, 'gzip', 'zlib'])
    def test_get_index_timestamps(self, s3_client, compression):
        # given
        utils.set_s3_index(
            s3_client,
            {
                'hello': {
                    'remote_timestamp': 1234,
                    'local_timestamp': 1200,
                },
                'world': {
                    'remote_timestamp': 5000,
                },
            },
            compression=compression,
        )

        # then
        assert s3_client.get_remote_timestamp('hello') == 1234
        assert s3_client.get_index_local_timestamp('hello') == 1200

        assert s3_client.get_remote_timestamp('world') == 5000
        assert s3_client.get_index_local_timestamp('world') is None

    def test_get_all_index_timestamps(self, s3_client):
        # given
        utils.set_s3_index(s3_client, {
            'hello': {
                'local_timestamp': 1200,
            },
            'world': {
                'local_timestamp': 4000,
            }
        })

        # then
        expected_output = {
            'hello': 1200,
            'world': 4000,
        }
        actual_output = s3_client.get_all_index_local_timestamps()
        assert actual_output == expected_output

    def test_update_index(self, s3_client):
        # given
        utils.set_s3_index(s3_client, {
            'red': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
            'green': {
                'remote_timestamp': 5000,
            }
        })

        utils.set_s3_contents(s3_client, 'red', timestamp=5001)
        utils.set_s3_contents(s3_client, 'yellow', timestamp=1000)
        utils.set_s3_contents(s3_client, 'orange', timestamp=2000)

        # when
        s3_client.update_index()

        # then
        expected_index = {
            'red': {
                'remote_timestamp': 1234,
                'local_timestamp': 5001,
            },
            'green': {
                'remote_timestamp': 5000,
                'local_timestamp': None,
            },
            'yellow': {
                'remote_timestamp': None,
                'local_timestamp': 1000,
            },
            'orange': {
                'remote_timestamp': None,
                'local_timestamp': 2000,
            },
        }
        assert s3_client.index == expected_index

    def test_get_real_local_timestamp(self, s3_client):
        # given
        utils.set_s3_contents(s3_client, 'orange', timestamp=2000)

        # then
        assert s3_client.get_real_local_timestamp('orange') == 2000
        assert s3_client.get_real_local_timestamp('idontexist') is None

    def test_get_all_real_local_timestamps(self, s3_client):
        # given
        utils.set_s3_contents(s3_client, 'chocolate/oranges', timestamp=2600)
        utils.set_s3_contents(s3_client, 'gummy/bears', timestamp=2400)
        utils.set_s3_contents(s3_client, 'carrot_cake', timestamp=2000)
        utils.set_s3_contents(s3_client, '.index', timestamp=2043)
        s3_client.reload_index()

        # then
        expected_output = {
            'chocolate/oranges': 2600,
            'gummy/bears': 2400,
            'carrot_cake': 2000,
        }
        actual_output = s3_client.get_all_real_local_timestamps()
        assert actual_output == expected_output

    def test_set_index_timestamps(self, s3_client):
        # given
        utils.set_s3_index(s3_client, {
            'red': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
        })

        # when
        s3_client.set_index_local_timestamp('red', 3000)
        s3_client.set_remote_timestamp('red', 4000)
        s3_client.set_index_local_timestamp('green', 5000)
        s3_client.set_remote_timestamp('yellow', 6000)

        # then
        expected_index = {
            'red': {
                'local_timestamp': 3000,
                'remote_timestamp': 4000,
            },
            'green': {
                'local_timestamp': 5000,
            },
            'yellow': {
                'remote_timestamp': 6000,
            },
        }
        assert s3_client.index == expected_index

    def test_interrupted_put(self, s3_client):
        utils.set_s3_contents(s3_client, 'keychain', data='iamsomedatathatexists')

        sync_object = SyncObject(utils.InterruptedBytesIO(), 900000, 3000)

        with pytest.raises(ValueError):
            s3_client.put('keychain', sync_object)

        result = s3_client.get('keychain')
        assert result.fp.read() == b'iamsomedatathatexists'

    def test_ignore_files(self, s3_client):
        utils.set_s3_contents(
            s3_client, '.syncignore', timestamp=4000, data=(
                '*~\n'
                'trashdirectory\n'
                '*py[oc]\n'
                '.git\n'
            )
        )
        s3_client.reload_ignore_files()

        utils.set_s3_contents(s3_client, '.zshrc~')
        utils.set_s3_contents(s3_client, 'trashdirectory')
        utils.set_s3_contents(s3_client, 'foo/mobile.py', timestamp=1290)
        utils.set_s3_contents(s3_client, 'foo/mobile.pyc')
        utils.set_s3_contents(s3_client, 'foo/mobile.pyo')
        utils.set_s3_contents(s3_client, 'foo/.git/7f04e3b69963e5d51b479b30591ab1c2b6a3e7')
        utils.set_s3_contents(s3_client, '.zshrc', timestamp=9999)

        s3_client.update_index()

        expected_index = {
            '.syncignore': {
                'local_timestamp': 4000,
                'remote_timestamp': None,
            },
            '.zshrc': {
                'local_timestamp': 9999,
                'remote_timestamp': None,
            },
            'foo/mobile.py': {
                'local_timestamp': 1290,
                'remote_timestamp': None,
            }
        }
        assert s3_client.index == expected_index
