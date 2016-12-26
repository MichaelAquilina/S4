# -*- coding: utf-8 -*-
from s3backup import StateAction, SyncAction, compare_actions, compare_states


class TestCompareStates(object):
    def test_both_empty(self):
        assert list(compare_states({}, {})) == []

    def test_empty_current(self):
        current = {}
        previous = {
            'orange': {'local_timestamp': 99999},
            'apple': {'local_timestamp': 88888},
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'apple': StateAction(StateAction.DELETE, None),
            'orange': StateAction(StateAction.DELETE, None),
        }
        assert actual_output == expected_output

    def test_empty_previous(self):
        current = {
            'foo': {'local_timestamp': 400123},
            'bar': {'local_timestamp': 23231},
        }
        previous = {}

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'foo': StateAction(StateAction.UPDATE, 400123),
            'bar': StateAction(StateAction.UPDATE, 23231),
        }
        assert actual_output == expected_output

    def test_new_current(self):
        current = {
            'red': {'local_timestamp': 1234567},
        }
        previous = {
            'red': {'local_timestamp': 1000000},
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'red': StateAction(StateAction.UPDATE, 1234567),
        }
        assert actual_output == expected_output

    def test_new_previous(self):
        current = {
            'monkey': {'local_timestamp': 8000},
        }
        previous = {
            'monkey': {'local_timestamp': 1000000},
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'monkey': StateAction(StateAction.CONFLICT, 1000000)
        }
        assert actual_output == expected_output

    def test_mixed(self):
        current = {
            'monkey': {'local_timestamp': 8000},
            'elephant': {'local_timestamp': 3232323},
            'dog': {'local_timestamp': 23233232323},
        }
        previous = {
            'monkey': {'local_timestamp': 1000000},
            'snake': {'local_timestamp': 232323},
            'dog': {'local_timestamp': 2333}
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'elephant': StateAction(StateAction.UPDATE, 3232323),
            'monkey': StateAction(StateAction.CONFLICT, 1000000),
            'snake': StateAction(StateAction.DELETE, None),
            'dog': StateAction(StateAction.UPDATE, 23233232323),
        }
        assert actual_output == expected_output
