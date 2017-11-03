# -*- encoding: utf-8 -*-

import argparse

from s4.commands.ls_command import LsCommand
from tests.utils import (
    create_logger,
    get_timestamp,
    set_local_index,
    set_s3_index,
)


class TestLsCommand(object):
    def test_empty_config(self, capsys):
        args = argparse.Namespace(target='idontexist')
        command = LsCommand(args, {}, create_logger())
        command.run()
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
        command = LsCommand(args, config, create_logger())
        command.run()
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
        command = LsCommand(args, config, create_logger())
        command.run()

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
        set_s3_index(s3_client, {
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
        set_local_index(local_client, {
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
        command = LsCommand(args, config, create_logger())
        command.run()

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
        set_s3_index(s3_client, {
            'cheese': {
                'local_timestamp': get_timestamp(2017, 12, 12, 8, 30)
            },
            'crackers': {
                'local_timestamp': None,
            }
        })
        set_local_index(local_client, {
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
        command = LsCommand(args, config, create_logger())
        command.run()

        out, err = capsys.readouterr()

        assert err == ""
        assert out == (
            'key       local                s3\n'
            '--------  -------------------  -------------------\n'
            'cheese    2017-02-02 08:30:00  2017-12-12 08:30:00\n'
            'crackers  <deleted>\n'

        )
