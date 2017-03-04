# -*- coding: utf-8 -*-

import argparse
import io
import logging

import pytest

import s3b


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
