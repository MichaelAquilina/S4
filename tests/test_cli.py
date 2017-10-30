# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import tempfile
from datetime import datetime

from inotify_simple import Event, flags
import mock
import pytest
import pytz

from s4 import cli, sync
from s4.clients import SyncState
from s4.resolution import Resolution
from s4.utils import to_timestamp
from tests import utils


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


def create_logger():
    result = logging.getLogger('create_logger')
    result.setLevel(logging.INFO)
    result.handlers = []
    result.addHandler(logging.StreamHandler())
    return result


def get_timestamp(year, month, day, hour, minute):
    return to_timestamp(
        datetime(year, month, day, hour, minute, tzinfo=pytz.UTC)
    )


@mock.patch('s4.utils.get_input')
class TestHandleConflict(object):
    def test_first_choice(self, get_input, s3_client, local_client):
        get_input.return_value = '1'

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = cli.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result.action == Resolution.UPDATE

    def test_second_choice(self, get_input, s3_client, local_client):
        get_input.return_value = '2'

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = cli.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result.action == Resolution.DELETE

    def test_skip(self, get_input, s3_client, local_client):
        get_input.return_value = 'X'

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = cli.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result is None

    @mock.patch('s4.cli.show_diff')
    def test_diff(self, show_diff, get_input, s3_client, local_client):
        get_input.side_effect = FakeInputStream([
            'd',
            'X',
        ])

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = cli.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result is None
        assert show_diff.call_count == 1
        show_diff.assert_called_with(s3_client, local_client, 'movie')


class TestShowDiff(object):
    @mock.patch('shutil.which')
    def test_diff_not_found(self, which, capsys, local_client, s3_client):
        which.return_value = None
        cli.show_diff(local_client, s3_client, "something")

        out, err = capsys.readouterr()
        assert out == (
            'Missing required "diff" executable.\n'
            "Install this using your distribution's package manager\n"
        )

    @mock.patch('shutil.which')
    def test_less_not_found(self, which, capsys, local_client, s3_client):
        def missing_less(value):
            return None if value == 'less' else 'something'

        which.side_effect = missing_less
        cli.show_diff(local_client, s3_client, "something")

        out, err = capsys.readouterr()
        assert out == (
            'Missing required "less" executable.\n'
            "Install this using your distribution's package manager\n"
        )

    @mock.patch('subprocess.call')
    def test_diff(self, call, local_client, s3_client):
        utils.set_local_contents(local_client, "something", 4000, "wow")
        utils.set_s3_contents(s3_client, "something", 3000, "nice")

        cli.show_diff(local_client, s3_client, "something")

        assert call.call_count == 2
        assert call.call_args_list[0][0][0][0] == "diff"
        assert call.call_args_list[1][0][0][0] == "less"


def test_progressbar_smoketest(s3_client, local_client):
    # Just test that nothing blows up
    utils.set_local_contents(
        local_client, 'history.txt',
        data='a long long time ago',
        timestamp=5000,
    )

    worker = sync.SyncWorker(
        s3_client,
        local_client,
        start_callback=cli.display_progress_bar,
        update_callback=cli.update_progress_bar,
        complete_callback=cli.hide_progress_bar,
    )
    worker.sync()


# TODO: Should catch KeyboardExceptions and raise them again
class TestMain(object):

    @mock.patch('argparse.ArgumentParser.print_help')
    def test_no_arguments_prints_help(self, print_help):
        cli.main([])
        assert print_help.call_count == 1

    @pytest.mark.parametrize(['loglevel'], [('INFO', ), ('DEBUG', )])
    @mock.patch('logging.basicConfig')
    def test_timestamps(self, basicConfig, loglevel):
        cli.main(['--timestamps', '--log-level', loglevel, 'version'])
        assert basicConfig.call_args[1]['format'].startswith('%(asctime)s: ')

    @mock.patch('logging.basicConfig')
    def test_debug_loglevel(self, basicConfig):
        cli.main(['--log-level=DEBUG', 'version'])
        assert basicConfig.call_args[1]['format'].startswith('%(levelname)s:%(module)s')
        assert basicConfig.call_args[1]['level'] == 'DEBUG'

    def test_version_command(self, capsys):
        cli.main(['version'])
        out, err = capsys.readouterr()
        assert out == '{}\n'.format(cli.VERSION)

    @mock.patch('s4.cli.ls_command')
    def test_ls_command(self, ls_command):
        cli.main(['ls', 'foo'])
        assert ls_command.call_count == 1

    @mock.patch('s4.cli.daemon_command')
    def test_daemon_command(self, daemon_command):
        cli.main(['daemon'])
        assert daemon_command.call_count == 1

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
        assert cli.get_config() == {'targets': {}}

    def test_correct_output(self, config_file):
        with open(config_file, 'w') as fp:
            json.dump({'local_folder': '/home/someone/something'}, fp)

        assert cli.get_config() == {'local_folder': '/home/someone/something'}


class FakeINotify(object):
    def __init__(self, events, wd_map):
        self.events = events
        self.wd_map = wd_map

    def add_watches(self, *args, **kwargs):
        return self.wd_map

    def read(self, *args, **kwargs):
        return self.events


@mock.patch('s4.sync.SyncWorker')
@mock.patch('s4.cli.INotifyRecursive')
class TestDaemonCommand(object):
    def single_term(self, index):
        """Simple terminator for the daemon command"""
        return index >= 1

    @pytest.mark.timeout(5)
    def test_no_targets(self, INotifyRecursive, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts='ignore', read_delay=0)
        cli.daemon_command(args, {'targets': {}}, create_logger(), terminator=self.single_term)

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'No targets available\n'
            'Use "add" command first\n'
        )
        assert SyncWorker.call_count == 0
        assert INotifyRecursive.call_count == 0

    @pytest.mark.timeout(5)
    def test_wrong_target(self, INotifyRecursive, SyncWorker, capsys):
        args = argparse.Namespace(targets=['foo'], conflicts='ignore', read_delay=0)
        cli.daemon_command(
            args,
            {'targets': {'bar': {}}},
            create_logger(),
            terminator=self.single_term
        )

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'Unknown target: foo\n'
        )
        assert SyncWorker.call_count == 0
        assert INotifyRecursive.call_count == 0

    @pytest.mark.timeout(5)
    def test_specific_target(self, INotifyRecursive, SyncWorker):
        INotifyRecursive.return_value = FakeINotify(
            events={
                Event(wd=1, mask=flags.CREATE, cookie=None, name="hello.txt"),
                Event(wd=2, mask=flags.CREATE, cookie=None, name="bar.txt"),
            },
            wd_map={
                1: '/home/jon/code/',
                2: '/home/jon/code/hoot',
            }
        )

        args = argparse.Namespace(targets=['foo'], conflicts='ignore', read_delay=0)
        config = {
            'targets': {
                'foo': {
                    'local_folder': '/home/jon/code',
                    's3_uri': 's3://bucket/code',
                    'aws_secret_access_key': '23232323',
                    'aws_access_key_id': '########',
                    'region_name': 'eu-west-2',
                },
                'bar': {},
            }
        }
        cli.daemon_command(args, config, create_logger(), terminator=self.single_term)

        assert SyncWorker.call_count == 2
        assert INotifyRecursive.call_count == 1


@mock.patch('s4.sync.SyncWorker')
class TestSyncCommand(object):
    def test_no_targets(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts=None, dry_run=False)
        cli.sync_command(args, {'targets': {}}, create_logger())
        out, err = capsys.readouterr()
        assert out == err == ''
        assert SyncWorker.call_count == 0

    def test_wrong_target(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=['foo', 'bar'], conflicts=None, dry_run=False)
        cli.sync_command(args, {'targets': {'baz': {}}}, create_logger())
        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"bar" is an unknown target. Choices are: [\'baz\']\n'
            '"foo" is an unknown target. Choices are: [\'baz\']\n'
        )
        assert SyncWorker.call_count == 0

    def test_sync_error(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts=None, dry_run=False, log_level="INFO")
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

        cli.sync_command(args,  config, create_logger())
        out, err = capsys.readouterr()
        assert SyncWorker.call_count == 2
        assert out == ''
        assert err == (
            "There was an error syncing 'bar': something bad happened\n"
            "There was an error syncing 'foo': something bad happened\n"
        )

    def test_sync_error_debug(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts=None, dry_run=False, log_level="DEBUG")
        config = {
            'targets': {
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

        cli.sync_command(args,  config, create_logger())

        out, err = capsys.readouterr()
        assert SyncWorker.call_count == 1
        assert out == ''
        assert err.split('\n')[:2] == [
            "something bad happened",
            "Traceback (most recent call last):",
        ]

    def test_keyboard_interrupt(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts=None, dry_run=False)
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

        cli.sync_command(args,  config, create_logger())
        out, err = capsys.readouterr()
        assert SyncWorker.call_count == 1
        assert out == ''
        assert err == (
            'Quitting due to Keyboard Interrupt...\n'
        )

    def test_all_targets(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts=None, dry_run=False)
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

        cli.sync_command(args,  config, create_logger())
        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'Syncing bar [/home/mike/barmil/ <=> s3://foobar/barrel/]\n'
            'Syncing foo [/home/mike/docs/ <=> s3://foobar/docs/]\n'
        )
        assert SyncWorker.call_count == 2


@mock.patch('s4.utils.get_input')
class TestEditCommand(object):
    def test_no_targets(self, get_input, capsys):
        cli.edit_command(None, {}, create_logger())
        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'You have not added any targets yet\n'
            'Use the "add" command to do this\n'
        )

    def test_missing_target(self, get_input, capsys):
        args = argparse.Namespace(target='idontexist')
        config = {
            'targets': {'foo': {}}
        }
        cli.edit_command(args, config, create_logger())

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"idontexist" is an unknown target\n'
            'Choices are: [\'foo\']\n'
        )

    def test_no_changes(self, get_input, config_file):
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
        cli.edit_command(args, config, create_logger())

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

    def test_correct_output(self, get_input, config_file):
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
        cli.edit_command(args, config, create_logger())

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
    def test_correct_behaviour(self, get_input, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Documents',
            's3://mybucket/Documents',
            'eu-west-2',
            'aaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbb',
            '',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials=None)

        cli.add_command(args, {'targets': {}}, create_logger())

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

    def test_copy_target_credentials(self, get_input, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Animals',
            's3://mybucket/Zoo',
            'us-west-2',
            'Beasts',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials='bar')

        cli.add_command(
            args,
            {
                'targets': {
                    'bar': {
                        'aws_secret_access_key': 'bar-secretz',
                        'aws_access_key_id': 'so-much-bar',
                    }
                }
            },
            create_logger()
        )

        with open(config_file, 'r') as fp:
            new_config = json.load(fp)

        expected_config = {
            'targets': {
                'bar': {
                    'aws_access_key_id': 'so-much-bar',
                    'aws_secret_access_key': 'bar-secretz',
                },
                'Beasts': {
                    'local_folder': '/home/user/Animals',
                    's3_uri': 's3://mybucket/Zoo',
                    'aws_access_key_id': 'so-much-bar',
                    'aws_secret_access_key': 'bar-secretz',
                    'region_name': 'us-west-2'
                }
            }
        }
        assert new_config == expected_config

    def test_copy_target_credentials_bad_target(self, get_input, capsys):
        fake_stream = FakeInputStream([
            '/home/user/Animals',
            's3://mybucket/Zoo',
            'us-west-2',
            'Beasts',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials='Foo')

        cli.add_command(args, {'targets': {'bar': {}}}, create_logger())

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"Foo" is an unknown target\n'
            'Choices are: [\'bar\']\n'
        )

    def test_custom_target_name(self, get_input, config_file):
        fake_stream = FakeInputStream([
            '/home/user/Music',
            's3://mybucket/Musiccccc',
            'us-west-1',
            '1234567890',
            'abcdefghij',
            'Tunes',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials=None)

        cli.add_command(args, {'targets': {}}, create_logger())

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
    def test_empty_config(self, capsys):
        args = argparse.Namespace(target='idontexist')
        cli.ls_command(args, {}, create_logger())
        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'You have not added any targets yet\n'
            'Use the "add" command to do this\n'
        )

    def test_missing_target(self, capsys):
        args = argparse.Namespace(target='idontexist')
        config = {
            'targets': {
                'foo': {},
                'bar': {},
            }
        }
        cli.ls_command(args, config, create_logger())
        out, err = capsys.readouterr()
        assert out == ""
        assert err == (
            '"idontexist" is an unknown target\n'
            'Choices are: [\'bar\', \'foo\']\n'
        )

    def test_correct_output_empty(self, s3_client, local_client, capsys):
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
        cli.ls_command(args, config, create_logger())

        out, err = capsys.readouterr()

        assert err == ""
        assert out == (
            'key    local    s3\n'
            '-----  -------  ----\n'
        )

    def test_correct_output_nonempty(self, s3_client, local_client, capsys):
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
        cli.ls_command(args, config, create_logger())

        out, err = capsys.readouterr()

        assert err == ""
        assert out == (
            'key    local                s3\n'
            '-----  -------------------  -------------------\n'
            'honey  2016-11-10 18:40:00  2016-12-12 08:30:00\n'
            'lemon  2017-02-02 08:30:00\n'
            'milk   2016-12-12 08:30:00  1989-10-23 11:30:00\n'

        )

    def test_show_all(self, s3_client, local_client, capsys):
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
        cli.ls_command(args, config, create_logger())

        out, err = capsys.readouterr()

        assert err == ""
        assert out == (
            'key       local                s3\n'
            '--------  -------------------  -------------------\n'
            'cheese    2017-02-02 08:30:00  2017-12-12 08:30:00\n'
            'crackers  <deleted>\n'

        )


class TestTargetsCommand(object):

    def test_empty(self, capsys):
        cli.targets_command(None, {'targets': {}}, create_logger())
        out, err = capsys.readouterr()
        assert out == err == ''

    def test_correct_output(self, capsys):
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

        cli.targets_command(None, config, create_logger())

        out, err = capsys.readouterr()
        assert err == ''
        assert out == (
            'Personal: [/home/user/Documents <=> s3://mybackup/Personal]\n'
            'Studies: [/media/backup/Studies <=> s3://something/something/Studies]\n'
        )


class TestRemoveCommand(object):
    def test_empty(self, capsys, config_file):
        args = argparse.Namespace(target='foo')
        cli.rm_command(args, {}, create_logger())

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'You have not added any targets yet\n'
        )

    def test_missing(self, capsys, config_file):
        args = argparse.Namespace(target='foo')
        cli.rm_command(args, {
            'targets': {
                'bar': {}
            }
        }, create_logger())

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"foo" is an unknown target\n'
            'Choices are: [\'bar\']\n'
        )

    def test_remove_target(self, capsys, config_file):
        args = argparse.Namespace(target='foo')
        cli.rm_command(args, {
            'targets': {
                'bar': {},
                'foo': {},
            }
        }, create_logger())

        out, err = capsys.readouterr()
        assert out == err == ''

        with open(config_file, 'rt') as fp:
            new_config = json.load(fp)

        expected_config = {'targets': {'bar': {}}}
        assert new_config == expected_config
