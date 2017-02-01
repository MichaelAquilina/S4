# -*- coding: utf-8 -*-
import datetime
import json
import io
import os

import boto3

from botocore.exceptions import ClientError

import freezegun

from moto import mock_s3

import pytest

from s3backup.clients import s3, SyncObject
from utils import set_s3_contents, set_s3_index, write_s3



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
        assert output_object.timestamp == s3.to_timestamp(frozen_time)

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
        assert output_object.timestamp == s3.to_timestamp(frozen_time)

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
        set_s3_contents(s3_client, 'war.png')
        set_s3_contents(s3_client, 'this/is/fine')
        set_s3_contents(s3_client, '.index')
        write_s3(
            s3_client.boto,
            s3_client.bucket,
            os.path.join('iamsomethingelse', 'ishouldnotshow.txt'),
        )

        # when
        actual_output = s3_client.get_local_keys()

        # then
        expected_output = ['war.png', 'this/is/fine']
        assert sorted(actual_output) == sorted(expected_output)

    def test_get_index_timestamps(self, s3_client):
        # given
        set_s3_index(s3_client, {
            'hello': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
            'world': {
                'remote_timestamp': 5000,
            }
        })

        # then
        assert s3_client.get_remote_timestamp('hello') == 1234
        assert s3_client.get_index_local_timestamp('hello') == 1200

        assert s3_client.get_remote_timestamp('world') == 5000
        assert s3_client.get_index_local_timestamp('world') is None

    def test_get_all_index_timestamps(self, s3_client):
        # given
        set_s3_index(s3_client, {
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
        set_s3_index(s3_client, {
            'red': {
                'remote_timestamp': 1234,
                'local_timestamp': 1200,
            },
            'green': {
                'remote_timestamp': 5000,
            }
        })

        set_s3_contents(s3_client, 'red', timestamp=5001)
        set_s3_contents(s3_client, 'yellow', timestamp=1000)
        set_s3_contents(s3_client, 'orange', timestamp=2000)

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
        set_s3_contents(s3_client, 'orange', timestamp=2000)

        # then
        assert s3_client.get_real_local_timestamp('orange') == 2000
        assert s3_client.get_real_local_timestamp('idontexist') is None

    def test_get_all_real_local_timestamps(self, s3_client):
        # given
        set_s3_contents(s3_client, 'chocolate/oranges', timestamp=2600)
        set_s3_contents(s3_client, 'gummy/bears', timestamp=2400)
        set_s3_contents(s3_client, 'carrot_cake', timestamp=2000)
        set_s3_contents(s3_client, '.index', timestamp=2043)
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
        set_s3_index(s3_client, {
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
