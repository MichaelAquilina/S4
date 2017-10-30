#! -*- encoding: utf8 -*-

import mock

from s4 import diff
from tests import utils


class TestShowDiff(object):
    @mock.patch('shutil.which')
    def test_diff_not_found(self, which, capsys, local_client, s3_client):
        which.return_value = None
        diff.show_diff(local_client, s3_client, "something")

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
        diff.show_diff(local_client, s3_client, "something")

        out, err = capsys.readouterr()
        assert out == (
            'Missing required "less" executable.\n'
            "Install this using your distribution's package manager\n"
        )

    @mock.patch('subprocess.call')
    def test_diff(self, call, local_client, s3_client):
        utils.set_local_contents(local_client, "something", 4000, "wow")
        utils.set_s3_contents(s3_client, "something", 3000, "nice")

        diff.show_diff(local_client, s3_client, "something")

        assert call.call_count == 2
        assert call.call_args_list[0][0][0][0] == "diff"
        assert call.call_args_list[1][0][0][0] == "less"
