# -*- encoding: utf-8 -*-

from s4.commands.targets_command import TargetsCommand

from tests.utils import create_logger


class TestTargetsCommand(object):

    def test_empty(self, capsys):
        command = TargetsCommand(None, {'targets': {}}, create_logger())
        command.run()
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

        command = TargetsCommand(None, config, create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert err == ''
        assert out == (
            'Personal: [/home/user/Documents <=> s3://mybackup/Personal]\n'
            'Studies: [/media/backup/Studies <=> s3://something/something/Studies]\n'
        )
