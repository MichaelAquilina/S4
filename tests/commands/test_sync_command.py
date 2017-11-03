#! -*- encoding: utf-8 -*-
import argparse

import mock

from s4.clients import SyncState
from s4.commands.sync_command import (
    SyncCommand,
    display_progress_bar,
    handle_conflict,
    hide_progress_bar,
    update_progress_bar,
)
from s4.resolution import Resolution
from s4.sync import SyncWorker

from tests.utils import FakeInputStream, create_logger, set_local_contents


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

        result = handle_conflict(
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

        result = handle_conflict(
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

        result = handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result is None

    @mock.patch('s4.commands.sync_command.show_diff')
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

        result = handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result is None
        assert show_diff.call_count == 1
        show_diff.assert_called_with(s3_client, local_client, 'movie')


def test_progressbar_smoketest(s3_client, local_client):
    # Just test that nothing blows up
    set_local_contents(
        local_client, 'history.txt',
        data='a long long time ago',
        timestamp=5000,
    )

    worker = SyncWorker(
        s3_client,
        local_client,
        start_callback=display_progress_bar,
        update_callback=update_progress_bar,
        complete_callback=hide_progress_bar,
    )
    worker.sync()


@mock.patch('s4.sync.SyncWorker')
class TestSyncCommand(object):
    def test_no_targets(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts=None, dry_run=False)
        command = SyncCommand(args, {'targets': {}}, create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == err == ''
        assert SyncWorker.call_count == 0

    def test_wrong_target(self, SyncWorker, capsys):
        args = argparse.Namespace(targets=['foo', 'bar'], conflicts=None, dry_run=False)
        command = SyncCommand(args, {'targets': {'baz': {}}}, create_logger())
        command.run()

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

        command = SyncCommand(args,  config, create_logger())
        command.run()

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

        command = SyncCommand(args,  config, create_logger())
        command.run()

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

        command = SyncCommand(args,  config, create_logger())
        command.run()

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

        command = SyncCommand(args,  config, create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'Syncing bar [/home/mike/barmil/ <=> s3://foobar/barrel/]\n'
            'Syncing foo [/home/mike/docs/ <=> s3://foobar/docs/]\n'
        )
        assert SyncWorker.call_count == 2
