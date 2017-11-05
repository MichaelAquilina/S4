# -*- encoding: utf-8 -*-
import argparse
import json

import mock

from s4.commands.edit_command import EditCommand

from tests import utils


@mock.patch('s4.utils.get_input')
class TestEditCommand(object):
    def test_no_targets(self, get_input, capsys):
        command = EditCommand(None, {}, utils.create_logger())
        command.run()
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
        command = EditCommand(args, config, utils.create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"idontexist" is an unknown target\n'
            'Choices are: [\'foo\']\n'
        )

    def test_no_changes(self, get_input, config_file):
        fake_stream = utils.FakeInputStream([
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
        command = EditCommand(args, config, utils.create_logger())
        command.run()

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
        fake_stream = utils.FakeInputStream([
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
        command = EditCommand(args, config, utils.create_logger())
        command.run()

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
