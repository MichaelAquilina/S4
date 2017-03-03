# -*- coding: utf-8 -*-

import io
import logging

import s3b


class TestTargetsCommand(object):

    def setup_method(self):
        self.fp = io.StringIO()
        self.logger = logging.getLogger('test_targets')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler(self.fp))

    def test_empty(self):
        s3b.targets_command(None, {}, self.logger)
        assert self.fp.getvalue() == ''

    def test_correct_output(self):
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

        s3b.targets_command(None, config, self.logger)

        expected_result = (
            'Personal: [/home/user/Documents <=> s3://mybackup/Personal]\n'
            'Studies: [/media/backup/Studies <=> s3://something/something/Studies]\n'
        )
        assert self.fp.getvalue() == expected_result
