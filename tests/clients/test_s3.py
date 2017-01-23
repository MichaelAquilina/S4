# -*- coding: utf-8 -*-
import datetime
import json
import io

import boto3

from botocore.exceptions import ClientError

import freezegun

from moto import mock_s3

import pytest

from s3backup.clients import s3, SyncState, SyncObject


def touch(client, bucket, key, timestamp=None):
    if timestamp is None:
        last_modified = datetime.datetime.utcnow()
    else:
        last_modified = datetime.datetime.utcfromtimestamp(timestamp)
    with freezegun.freeze_time(last_modified):
        client.put_object(Bucket=bucket, Key=key)


class TestParseS3URI(object):
    def test_empty(self):
        assert s3.parse_s3_uri('') is None

    def test_incorrect_format(self):
        assert s3.parse_s3_uri('nos3infront/some/path') is None
        assert s3.parse_s3_uri(':/232red+-32') is None

    def test_correct_output(self):
        actual_output = s3.parse_s3_uri('s3://fruit.bowl/apples/and/oranges/')
        assert actual_output.bucket == 'fruit.bowl'
        assert actual_output.key == 'apples/and/oranges/'


class TestS3SyncClient(object):
    @mock_s3
    def test_repr(self):
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        assert repr(client) == 'S3SyncClient<testbucket, foo/bar>'

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

    @mock_s3
    def test_put(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        data = b'munchkin'

        # when
        input_object = SyncObject(io.BytesIO(data), len(data), 4000)
        client.put('something/boardgame.rst', input_object)

        # then
        resp = s3_client.get_object(Bucket='testbucket', Key='foo/bar/something/boardgame.rst')
        assert resp['Body'].read() == data
        assert client.get_remote_timestamp('something/boardgame.rst') == 4000

    @mock_s3
    def test_get(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')

        data = b'#000000'
        frozen_time = datetime.datetime(2016, 10, 23, 10, 30, tzinfo=datetime.timezone.utc)
        with freezegun.freeze_time(frozen_time):
            s3_client.put_object(Bucket='testbucket', Key='foo/bar/black.color', Body=data)

        # when
        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        output_object = client.get('black.color')

        # then
        assert output_object.fp.read() == data
        assert output_object.total_size == len(data)
        assert output_object.timestamp == s3.to_timestamp(frozen_time)

    @mock_s3
    def test_put_get(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')

        data = b'woooooooooshhh'
        input_object = SyncObject(io.BytesIO(data), len(data), 4000)
        frozen_time = datetime.datetime(2016, 10, 23, 10, 30, tzinfo=datetime.timezone.utc)

        # when
        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        with freezegun.freeze_time(frozen_time):
            client.put('something/woosh.gif', input_object)

        # then
        output_object = client.get('something/woosh.gif')
        assert output_object.fp.read() == data
        assert output_object.timestamp == s3.to_timestamp(frozen_time)

    @mock_s3
    def test_delete(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/war.png', Body='bang')

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # when
        assert client.delete('war.png') is True

        # then
        with pytest.raises(ClientError) as exc:
            s3_client.head_object(Bucket='testbucket', Key='foo/bar/war.png')
        assert exc.value.response['Error']['Code'] == '404'

    @mock_s3
    def test_delete_non_existant(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')

        # when
        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # then
        assert client.delete('idontexist.png') is False

    @mock_s3
    def test_get_local_keys(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        touch(s3_client, 'testbucket', 'foo/bar/war.png')
        touch(s3_client, 'testbucket', 'foo/bar/this/is/fine')
        touch(s3_client, 'testbucket', 'foo/bar/.index')
        touch(s3_client, 'testbucket', 'foo/ishouldnotshow.txt')

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # when
        actual_output = client.get_local_keys()

        # then
        expected_output = ['war.png', 'this/is/fine']
        assert sorted(actual_output) == sorted(expected_output)

    @mock_s3
    def test_get_index_timestamps(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/.index', Body=json.dumps({
            'hello': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
            'world': {
                'remote_timestamp': 5000,
            }
        }))

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # then
        assert client.get_remote_timestamp('hello') == 1234
        assert client.get_index_local_timestamp('hello') == 1200

        assert client.get_remote_timestamp('world') == 5000
        assert client.get_index_local_timestamp('world') is None

    @mock_s3
    def test_get_all_index_timestamps(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/.index', Body=json.dumps({
            'hello': {
                'local_timestamp': 1200,
            },
            'world': {
                'local_timestamp': 4000,
            }
        }))

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # then
        expected_output = {
            'hello': 1200,
            'world': 4000,
        }
        actual_output = client.get_all_index_local_timestamps()
        assert actual_output == expected_output

    @mock_s3
    def test_update_index(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/.index', Body=json.dumps({
            'red': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
            'green': {
                'remote_timestamp': 5000,
            }
        }))
        touch(s3_client, 'testbucket', 'foo/bar/red', 5001)
        touch(s3_client, 'testbucket', 'foo/bar/yellow', 1000)
        touch(s3_client, 'testbucket', 'foo/bar/orange', 2000)

        # when
        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        client.update_index()

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
        assert client.index == expected_index

    @mock_s3
    def test_get_real_local_timestamp(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        touch(s3_client, 'testbucket', 'foo/bar/orange', 2000)

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # then
        assert client.get_real_local_timestamp('orange') == 2000
        assert client.get_real_local_timestamp('idontexist') is None

    @mock_s3
    def test_get_all_real_local_timestamps(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        touch(s3_client, 'testbucket', 'food/chocolate/oranges', 2600)
        touch(s3_client, 'testbucket', 'food/gummy/bears', 2400)
        touch(s3_client, 'testbucket', 'food/carrot_cake', 2000)
        touch(s3_client, 'testbucket', 'food/.index', 2043)

        client = s3.S3SyncClient(s3_client, 'testbucket', 'food')

        # then
        expected_output = {
            'chocolate/oranges': 2600,
            'gummy/bears': 2400,
            'carrot_cake': 2000,
        }
        actual_output = client.get_all_real_local_timestamps()
        assert actual_output == expected_output

    @mock_s3
    def test_set_index_timestamps(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/.index', Body=json.dumps({
            'red': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
        }))

        # when
        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        client.set_index_local_timestamp('red', 3000)
        client.set_remote_timestamp('red', 4000)
        client.set_index_local_timestamp('green', 5000)
        client.set_remote_timestamp('yellow', 6000)

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
        assert client.index == expected_index

    @mock_s3
    def test_get_action(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/.index', Body=json.dumps({
            'foo': {
                'local_timestamp': 4000,
            },
            'bar': {
                'local_timestamp': 1000,
                'remote_timestamp': 1100,
            },
            'baz': {
                'local_timestamp': 1111,
                'remote_timestamp': 1400,
            },
            'ooo': {
                'local_timestamp': 9999,
            },
            'ppp': {
                'local_timestamp': None,
                'remote_timestamp': 4000,
            }
        }))
        touch(s3_client, 'testbucket', 'foo/bar/foo', 5000)
        touch(s3_client, 'testbucket', 'foo/bar/baz', 1111)
        touch(s3_client, 'testbucket', 'foo/bar/ooo', 1000)

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        assert client.get_action('foo') == SyncState(SyncState.UPDATED, 5000)
        assert client.get_action('bar') == SyncState(SyncState.DELETED, 1100)
        assert client.get_action('baz') == SyncState(SyncState.NOCHANGES, 1400)
        assert client.get_action('ooo') == SyncState(SyncState.CONFLICT, 9999)
        assert client.get_action('ppp') == SyncState(SyncState.DELETED, 4000)
        assert client.get_action('dontexist') == SyncState(SyncState.DOESNOTEXIST, None)
