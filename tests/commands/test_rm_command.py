# -*- encoding: utf-8 -*-

import argparse
import json

from s4.commands.rm_command import RmCommand
from tests.utils import create_logger


class TestRemoveCommand(object):
    def test_empty(self, capsys, config_file):
        args = argparse.Namespace(target='foo')
        command = RmCommand(args, {}, create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            'You have not added any targets yet\n'
        )

    def test_missing(self, capsys, config_file):
        args = argparse.Namespace(target='foo')
        command = RmCommand(args, {
            'targets': {
                'bar': {}
            }
        }, create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"foo" is an unknown target\n'
            'Choices are: [\'bar\']\n'
        )

    def test_remove_target(self, capsys, config_file):
        args = argparse.Namespace(target='foo')
        command = RmCommand(args, {
            'targets': {
                'bar': {},
                'foo': {},
            }
        }, create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == err == ''

        with open(config_file, 'rt') as fp:
            new_config = json.load(fp)

        expected_config = {'targets': {'bar': {}}}
        assert new_config == expected_config
