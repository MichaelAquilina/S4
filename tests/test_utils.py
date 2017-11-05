# -*- coding: utf-8 -*-

import json

import mock

from s4 import utils


@mock.patch('getpass.getpass')
@mock.patch('builtins.input')
def test_get_input(getpass, input_fn):
    utils.get_input("give me some info", secret=False)

    assert getpass.call_count == 1
    assert input_fn.call_count == 0


@mock.patch('getpass.getpass')
@mock.patch('builtins.input')
def test_get_input_secret(getpass, input_fn):
    utils.get_input("give me some secret info", secret=True)

    assert getpass.call_count == 0
    assert input_fn.call_count == 1


class TestGetConfigFile(object):
    @mock.patch('s4.utils.CONFIG_FILE_PATH', '/i/dont/exist')
    def test_no_file(self):
        assert utils.get_config() == {'targets': {}}

    def test_correct_output(self, config_file):
        with open(config_file, 'w') as fp:
            json.dump({'local_folder': '/home/someone/something'}, fp)

        assert utils.get_config() == {'local_folder': '/home/someone/something'}
