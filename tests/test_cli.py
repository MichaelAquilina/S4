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

from s4 import cli
from s4.utils import to_timestamp
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
    fd, temp_path = tempfile.mkstemp()
    mocker = mock.patch('s4.cli.CONFIG_FILE_PATH', temp_path)
    mocker.start()
    yield temp_path
    mocker.stop()
    os.close(fd)
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


class TestMain(object):
    @mock.patch('argparse.ArgumentParser.print_help')
    def test_no_arguments_prints_help(self, print_help):
        cli.main([])
        assert print_help.call_count == 1

    def test_version_command(self, capsys):
        cli.main(['version'])
        out, err = capsys.readouterr()
        assert out == '{}\n'.format(cli.VERSION)

    @mock.patch('s4.cli.ls_command')
    def test_ls_command(self, ls_command):
        cli.main(['ls', 'foo'])
        assert ls_command.call_count == 1

    @mock.patch('s4.cli.sync_command')
    def test_sync_command(self, sync_command):
        cli.main(['sync', 'foo'])
        assert sync_command.call_count == 1

    @mock.patch('s4.cli.edit_command')
    def test_edit_command(self, edit_command):
        cli.main(['edit', 'foo'])
        assert edit_command.call_count == 1

    @mock.patch('s4.cli.targets_command')
    def test_targets_command(self, targets_command):
        cli.main(['targets'])
        assert targets_command.call_count == 1

    @mock.patch('s4.cli.rm_command')
    def test_rm_command(self, rm_command):
        cli.main(['rm', 'foo'])
        assert rm_command.call_count == 1

    @mock.patch('s4.cli.add_command')
    def test_add_command(self, add_command):
        cli.main(['add'])
        assert add_command.call_count == 1


class TestGetConfigFile(object):
    @mock.patch('s4.cli.CONFIG_FILE_PATH', '/i/dont/exist')
    def test_no_file(self):
        assert cli.get_config() == {}

    def test_correct_output(self, config_file):
        with open(config_file, 'w') as fp:
            json.dump({'local_folder': '/home/someone/something'}, fp)

        assert cli.get_config() == {'local_folder': '/home/someone/something'}

@mock.patch('s4.sync.SyncWorker')
class TestSyncCommand(object):
    def test_no_targets(self, SyncWorker, logger):
        args = argparse.Namespace(targets=None, conflicts=None)
        cli.sync_command(args, {'targets': {}}, logger)
        assert get_stream_value(logger) == ''
        assert SyncWorker.call_count == 0

    def test_wrong_target(self, SyncWorker, logger):
        args = argparse.Namespace(targets=['foo', 'bar'], conflicts=None)
        cli.sync_command(args, {'targets': {'baz': {}}}, logger)
        assert get_stream_value(logger) == (
            '"bar" is an unknown target. Choices are: [\'baz\']\n'
            '"foo" is an unknown target. Choices are: [\'baz\']\n'
        )
        assert SyncWorker.call_count == 0

    def test_sync_error(self, SyncWorker, logger):
        args = argparse.Namespace(targets=None, conflicts=None)
        config = {
            'targets': {
                'foo': {
                    'local_folder': '/home/mike/docs',
                    's3_uri': 's3://foobar/docs',
                    'aws_access_key_id': '3223323',
                    'aws_secret_access_key': '23#@423#@',
                    'region_name': 'us-east-1',
                },
                'bar': {
                    'local_folder': '/home/mike/barmil',
                    's3_uri': 's3://foobar/barrel',
                    'aws_access_key_id': '3223',
                    'aws_secret_access_key': '23#eWEa@423#@',
                    'region_name': 'us-west-2',
                }
            }
        }
        SyncWorker.side_effect = ValueError('something bad happened')

        cli.sync_command(args,  config, logger)
        assert SyncWorker.call_count == 2
        assert get_stream_value(logger) == (
            "There was an error syncing 'bar': something bad happened\n"
            "There was an error syncing 'foo': something bad happened\n"
        )

    def test_keyboard_interrupt(self, SyncWorker, logger):
        args = argparse.Namespace(targets=None, conflicts=None)
        config = {
            'targets': {
                'foo': {
                    'local_folder': '/home/mike/docs',
                    's3_uri': 's3://foobar/docs',
                    'aws_access_key_id': '3223323',
                    'aws_secret_access_key': '23#@423#@',
                    'region_name': 'us-east-1',
                },
                'bar': {
                    'local_folder': '/home/mike/barmil',
                    's3_uri': 's3://foobar/barrel',
                    'aws_access_key_id': '3223',
                    'aws_secret_access_key': '23#eWEa@423#@',
                    'region_name': 'us-west-2',
                }
            }
        }
        SyncWorker.side_effect = KeyboardInterrupt

        cli.sync_command(args,  config, logger)
        assert SyncWorker.call_count == 1
        assert get_stream_value(logger) == (
            'Quitting due to Keyboard Interrupt...\n'
        )

    def test_all_targets(self, SyncWorker, logger):
        args = argparse.Namespace(targets=None, conflicts=None)
        config = {
            'targets': {
                'foo': {
                    'local_folder': '/home/mike/docs',
                    's3_uri': 's3://foobar/docs',
                    'aws_access_key_id': '3223323',
                    'aws_secret_access_key': '23#@423#@',
                    'region_name': 'us-east-1',
                },
                'bar': {
                    'local_folder': '/home/mike/barmil',
                    's3_uri': 's3://foobar/barrel',
                    'aws_access_key_id': '3223',
                    'aws_secret_access_key': '23#eWEa@423#@',
                    'region_name': 'us-west-2',
                }
            }
        }

        cli.sync_command(args,  config, logger)
        assert get_stream_value(logger) == (
            'Syncing bar [/home/mike/barmil/ <=> s3://foobar/barrel/]\n'
            'Syncing foo [/home/mike/docs/ <=> s3://foobar/docs/]\n'
        )
        assert SyncWorker.call_count == 2


@mock.patch('s4.utils.get_input')
class TestEditCommand(object):
    def test_no_targets(self, get_input, logger):
        cli.edit_command(None, {}, logger)
        expected_result = (
            'You have not added any targets yet\n'
            'Use the "add" command to do this\n'
        )
        assert get_stream_value(logger) == expected_result

    def test_missing_target(self, get_input, logger):
        args = argparse.Namespace(target='idontexist')
        config = {
            'targets': {'foo': {}}
        }
        cli.edit_command(args, config, logger)

        expected_result = (
            '"idontexist" is an unknown target\n'
            'Choices are: [\'foo\']\n'
        )
        assert get_stream_value(logger) == expected_result

    def test_no_changes(self, get_input, logger, config_file):
        fake_stream = FakeInputStream([
            '',
            '',
            '',
            '',
            '',
        ])
        get_input.side_effect = fake_stream

        args = argparse.Namespace(target='foo')
        config = {
            'targets': {'foo': {
                'local_folder': '/home/mark/documents',
                's3_uri': 's3://buckets/mybackup',
                'aws_access_key_id': '23123123123313',
                'aws_secret_access_key': 'edwdadwadwadwdd',
                'region_name': 'eu-west-1',
            }}
        }
        cli.edit_command(args, config, logger)

        with open(config_file, 'r') as fp:
            config = json.load(fp)

        expected_config = {
            'targets': {'foo': {
                'local_folder': '/home/mark/documents',
                's3_uri': 's3://buckets/mybackup',
                'aws_access_key_id': '23123123123313',
                'aws_secret_access_key': 'edwdadwadwadwdd',
                'region_name': 'eu-west-1',
            }}
        }
        assert expected_config == config


    def test_correct_output(self, get_input, logger, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Documents',
            's3://buckets/mybackup222',
            '9999999999',
            'bbbbbbbbbbbbbbbbbbbbbbbb',
            'eu-west-2',
        ])
        get_input.side_effect = fake_stream

        args = argparse.Namespace(target='foo')
        config = {
            'targets': {'foo': {
                'local_folder': '/home/mark/documents',
                's3_uri': 's3://buckets/mybackup',
                'aws_access_key_id': '23123123123313',
                'aws_secret_access_key': 'edwdadwadwadwdd',
                'region_name': 'eu-west-1',
            }}
        }
        cli.edit_command(args, config, logger)

        with open(config_file, 'r') as fp:
            config = json.load(fp)

        expected_config = {
            'targets': {'foo': {
                'local_folder': '/home/user/Documents',
                's3_uri': 's3://buckets/mybackup222',
                'aws_access_key_id': '9999999999',
                'aws_secret_access_key': 'bbbbbbbbbbbbbbbbbbbbbbbb',
                'region_name': 'eu-west-2',
            }}
        }
        assert expected_config == config


@mock.patch('s4.utils.get_input')
class TestAddCommand(object):
    def test_correct_behaviour(self, get_input, logger, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Documents',
            's3://mybucket/Documents',
            'aaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbb',
            'eu-west-2',
            '',
        ])
        get_input.side_effect = fake_stream

        cli.add_command(None, {}, logger)

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

    def test_custom_target_name(self, get_input, logger, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Music',
            's3://mybucket/Musiccccc',
            '1234567890',
            'abcdefghij',
            'us-west-1',
            'Tunes',
        ])
        get_input.side_effect = fake_stream

        cli.add_command(None, {}, logger)

        with open(config_file, 'r') as fp:
            new_config = json.load(fp)

        expected_config = {
            'targets': {
                'Tunes': {
                    'local_folder': '/home/user/Music',
                    's3_uri': 's3://mybucket/Musiccccc',
                    'aws_access_key_id': '1234567890',
                    'aws_secret_access_key': 'abcdefghij',
                    'region_name': 'us-west-1'
                }
            }
        }
        assert new_config == expected_config


class TestLsCommand(object):
    def test_empty_config(self, logger):
        args = argparse.Namespace(target='idontexist')
        cli.ls_command(args, {}, logger)
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
        cli.ls_command(args, config, logger)
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

        args = argparse.Namespace(target='foo', sort_by='key', descending=False)
        cli.ls_command(args, config, logger)

        expected_result = (
            'key    local    s3\n'
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
            'ginger': {
                'local_timestamp': None,
            }
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
            'ginger': {
                'local_timestamp': None,
            }
        })

        args = argparse.Namespace(
            target='foo',
            sort_by='key',
            show_all=False,
            descending=False,
        )
        cli.ls_command(args, config, logger)

        expected_result = (
            'key    local                s3\n'
            '-----  -------------------  -------------------\n'
            'honey  2016-11-10 18:40:00  2016-12-12 08:30:00\n'
            'lemon  2017-02-02 08:30:00\n'
            'milk   2016-12-12 08:30:00  1989-10-23 11:30:00\n'

        )
        assert get_stream_value(logger) == expected_result

    def test_show_all(self, s3_client, local_client, logger):
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
            'cheese': {
                'local_timestamp': get_timestamp(2017, 12, 12, 8, 30)
            },
            'crackers': {
                'local_timestamp': None,
            }
        })
        utils.set_local_index(local_client, {
            'cheese': {
                'local_timestamp': get_timestamp(2017, 2, 2, 8, 30)
            },
            'crackers': {
                'local_timestamp': None,
            }
        })

        args = argparse.Namespace(
            target='foo',
            sort_by='key',
            show_all=True,
            descending=False,
        )
        cli.ls_command(args, config, logger)

        expected_result = (
            'key       local                s3\n'
            '--------  -------------------  -------------------\n'
            'cheese    2017-02-02 08:30:00  2017-12-12 08:30:00\n'
            'crackers  <deleted>\n'

        )
        assert get_stream_value(logger) == expected_result



class TestTargetsCommand(object):

    def test_empty(self, logger):
        cli.targets_command(None, {}, logger)
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

        cli.targets_command(None, config, logger)

        expected_result = (
            'Personal: [/home/user/Documents <=> s3://mybackup/Personal]\n'
            'Studies: [/media/backup/Studies <=> s3://something/something/Studies]\n'
        )
        assert get_stream_value(logger) == expected_result


class TestRemoveCommand(object):
    def test_empty(self, logger, config_file):
        args = argparse.Namespace(target='foo')
        cli.rm_command(args, {}, logger)

        expected_output = (
            'You have not added any targets yet\n'
        )
        assert get_stream_value(logger) == expected_output

    def test_missing(self, logger, config_file):
        args = argparse.Namespace(target='foo')
        cli.rm_command(args, {
            'targets': {
                'bar': {}
            }
        }, logger)

        expected_output = (
            '"foo" is an unknown target\n'
            'Choices are: [\'bar\']\n'
        )
        assert get_stream_value(logger) == expected_output

    def test_remove_target(self, logger, config_file):
        args = argparse.Namespace(target='foo')
        cli.rm_command(args, {
            'targets': {
                'bar': {},
                'foo': {},
            }
        }, logger)

        assert get_stream_value(logger) == ''

        with open(config_file, 'rt') as fp:
            new_config = json.load(fp)

        expected_config = {'targets': {'bar': {}}}
        assert new_config == expected_config
