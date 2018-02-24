# -*- encoding: utf-8 -*-

import argparse

from inotify_simple import Event, flags

import mock

import pytest

from s4.commands.daemon_command import DaemonCommand
from tests.utils import create_logger


class FakeINotify(object):
    def __init__(self, events, wd_map):
        self.events = events
        self.wd_map = wd_map

    def add_watches(self, *args, **kwargs):
        return self.wd_map

    def read(self, *args, **kwargs):
        return self.events


@mock.patch('s4.sync.SyncWorker')
@mock.patch('s4.commands.daemon_command.INotifyRecursive')
class TestDaemonCommand(object):
    def single_term(self, index):
        """Simple terminator for the daemon command"""
        return index >= 1

    @mock.patch('s4.commands.daemon_command.supported', False)
    def test_os_not_supported(self, INotifyRecursive, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts='ignore', read_delay=0)

        command = DaemonCommand(args, {}, create_logger())
        command.run(terminator=self.single_term)

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'Cannot run INotify on your operating system\n'
            'Only Linux machines are officially supported for this command\n'
        )
        assert SyncWorker.call_count == 0
        assert INotifyRecursive.call_count == 0

    @pytest.mark.timeout(5)
    def test_no_targets(self, INotifyRecursive, SyncWorker, capsys):
        args = argparse.Namespace(targets=None, conflicts='ignore', read_delay=0)

        command = DaemonCommand(args, {'targets': {}}, create_logger())
        command.run(terminator=self.single_term)

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

        command = DaemonCommand(
            args,
            {'targets': {'bar': {}}},
            create_logger(),
        )
        command.run(terminator=self.single_term)

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
        command = DaemonCommand(args, config, create_logger())
        command.run(terminator=self.single_term)

        assert SyncWorker.call_count == 2
        assert INotifyRecursive.call_count == 1
