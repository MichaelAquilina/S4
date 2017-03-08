# -*- coding: utf-8 -*-

import argparse
import io
import json
import logging
import os
import tempfile
from datetime import datetime

import mock
import pytest
import pytz

import s3b

from s3backup.utils import to_timestamp
import utils


class FakeInputStream(object):
    def __init__(self, results):
        self.results = results
        self.index = 0

    def __call__(self, *args, **kwargs):
        output = self.results[self.index]
        self.index += 1
        return output


@pytest.yield_fixture
def config_file():
    _, temp_path = tempfile.mkstemp()
    mocker = mock.patch('s3b.CONFIG_FILE_PATH', temp_path)
    mocker.start()
    yield temp_path
    mocker.stop()
    os.remove(temp_path)


@pytest.fixture
def logger():
    stream = io.StringIO()
    result = logging.getLogger('test_targets')
    result.setLevel(logging.INFO)
    result.handlers = []
    result.addHandler(logging.StreamHandler(stream))
    return result


def get_stream_value(logger):
    return logger.handlers[0].stream.getvalue()


def get_timestamp(year, month, day, hour, minute):
    return to_timestamp(
        datetime(year, month, day, hour, minute, tzinfo=pytz.UTC)
    )


class TestEditCommand(object):
    def test_no_targets(self, logger):
        s3b.edit_command(None, {}, logger)
        expected_result = (
            'You have not added any targets yet\n'
            'Use the "add" command to do this\n'
        )
        assert get_stream_value(logger) == expected_result

    def test_missing_target(self, logger):
        args = argparse.Namespace(target='idontexist')
        config = {
            'targets': {'foo': {}}
        }
        s3b.edit_command(args, config, logger)

        expected_result = (
            '"idontexist" is an unknown target\n'
            'Choices are: [\'foo\']\n'
        )
        assert get_stream_value(logger) == expected_result


@mock.patch('s3backup.utils.get_input')
@mock.patch('getpass.getpass')
class TestAddCommand(object):
    def test_correct_behaviour(self, get_input, getpass, logger, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Documents',
            's3://mybucket/Documents',
            'aaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbb',
            'eu-west-2',
            '',
        ])
        getpass.side_effect = fake_stream
        get_input.side_effect = fake_stream

        s3b.add_command(None, {}, logger)

        with open(config_file, 'r') as fp:
            new_config = json.load(fp)

        expected_config = {
            'targets': {
                'Documents': {
                    'local_folder': '/home/user/Documents',
                    's3_uri': 's3://mybucket/Documents',
                    'aws_access_key_id': 'aaaaaaaaaaaaaaaaaaaaaaaa',
                    'aws_secret_access_key': 'bbbbbbbbbbbbbbbbbbbbbbbb',
                    'region_name': 'eu-west-2'
                }
            }
        }
        assert new_config == expected_config


class TestLsCommand(object):
    def test_empty_config(self, logger):
        args = argparse.Namespace(target='idontexist')
        s3b.ls_command(args, {}, logger)
        expected_result = (
            'You have not added any targets yet\n'
            'Use the "add" command to do this\n'
        )
        assert get_stream_value(logger) == expected_result

    def test_missing_target(self, logger):
        args = argparse.Namespace(target='idontexist')
        config = {
            'targets': {
                'foo': {},
                'bar': {},
            }
        }
        s3b.ls_command(args, config, logger)
        expected_result = (
            '"idontexist" is an unknown target\n'
            'Choices are: [\'bar\', \'foo\']\n'
        )
        assert get_stream_value(logger) == expected_result

    def test_correct_output_empty(self, s3_client, local_client, logger):
        config = {
            'targets': {
                'foo': {
                    'local_folder': local_client.get_uri(),
                    's3_uri': s3_client.get_uri(),
                    'aws_access_key_id': '',
                    'aws_secret_access_key': '',
                    'region_name': 'eu-west-2',
                }
            }
        }

        args = argparse.Namespace(target='foo')
        s3b.ls_command(args, config, logger)

        expected_result = (
            'Key    local    s3\n'
            '-----  -------  ----\n'
        )
        assert get_stream_value(logger) == expected_result

    def test_correct_output_nonempty(self, s3_client, local_client, logger):
        config = {
            'targets': {
                'foo': {
                    'local_folder': local_client.get_uri(),
                    's3_uri': s3_client.get_uri(),
                    'aws_access_key_id': '',
                    'aws_secret_access_key': '',
                    'region_name': 'eu-west-2',
                }
            }
        }
        utils.set_s3_index(s3_client, {
            'milk': {
                'local_timestamp': get_timestamp(1989, 10, 23, 11, 30)
            },
            'honey': {
                'local_timestamp': get_timestamp(2016, 12, 12, 8, 30)
            },
        })
        utils.set_local_index(local_client, {
            'milk': {
                'local_timestamp': get_timestamp(2016, 12, 12, 8, 30)
            },
            'honey': {
                'local_timestamp': get_timestamp(2016, 11, 10, 18, 40)
            },
            'lemon': {
                'local_timestamp': get_timestamp(2017, 2, 2, 8, 30)
            },
        })

        args = argparse.Namespace(target='foo')
        s3b.ls_command(args, config, logger)

        expected_result = (
            'Key    local                s3\n'
            '-----  -------------------  -------------------\n'
            'honey  2016-11-10 18:40:00  2016-12-12 08:30:00\n'
            'lemon  2017-02-02 08:30:00\n'
            'milk   2016-12-12 08:30:00  1989-10-23 11:30:00\n'

        )
        assert get_stream_value(logger) == expected_result


class TestTargetsCommand(object):

    def test_empty(self, logger):
        s3b.targets_command(None, {}, logger)
        assert get_stream_value(logger) == ''

    def test_correct_output(self, logger):
        config = {
            'targets': {
                'Personal': {
                    's3_uri': 's3://mybackup/Personal',
                    'local_folder': '/home/user/Documents',
                },
                'Studies': {
                    's3_uri': 's3://something/something/Studies',
                    'local_folder': '/media/backup/Studies',
                },
            }
        }

        s3b.targets_command(None, config, logger)

        expected_result = (
            'Personal: [/home/user/Documents <=> s3://mybackup/Personal]\n'
            'Studies: [/media/backup/Studies <=> s3://something/something/Studies]\n'
        )
        assert get_stream_value(logger) == expected_result
