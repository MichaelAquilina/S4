# -*- coding: utf-8 -*-

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

