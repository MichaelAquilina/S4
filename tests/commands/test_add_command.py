# -*- encoding: utf-8 -*-

import argparse
import json

import mock

from s4.commands.add_command import AddCommand
from tests import utils


@mock.patch('s4.utils.get_input')
class TestAddCommand(object):
    def test_correct_behaviour(self, get_input, config_file):
        fake_stream = utils.FakeInputStream([
            '/home/user/Documents',
            's3://mybucket/Documents',
            'eu-west-2',
            'aaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbb',
            '',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials=None)

        command = AddCommand(args, {'targets': {}}, utils.create_logger())
        command.run()

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
        fake_stream = utils.FakeInputStream([
            '/home/user/Animals',
            's3://mybucket/Zoo',
            'us-west-2',
            'Beasts',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials='bar')

        command = AddCommand(
            args,
            {
                'targets': {
                    'bar': {
                        'aws_secret_access_key': 'bar-secretz',
                        'aws_access_key_id': 'so-much-bar',
                    }
                }
            },
            utils.create_logger()
        )
        command.run()

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
        fake_stream = utils.FakeInputStream([
            '/home/user/Animals',
            's3://mybucket/Zoo',
            'us-west-2',
            'Beasts',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials='Foo')

        command = AddCommand(args, {'targets': {'bar': {}}}, utils.create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == ''
        assert err == (
            '"Foo" is an unknown target\n'
            'Choices are: [\'bar\']\n'
        )

    def test_custom_target_name(self, get_input, config_file):
        fake_stream = utils.FakeInputStream([
            '/home/user/Music',
            's3://mybucket/Musiccccc',
            'us-west-1',
            '1234567890',
            'abcdefghij',
            'Tunes',
        ])
        get_input.side_effect = fake_stream
        args = argparse.Namespace(copy_target_credentials=None)

        command = AddCommand(args, {'targets': {}}, utils.create_logger())
        command.run()

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
