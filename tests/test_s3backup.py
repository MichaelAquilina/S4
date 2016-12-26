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


class TestCompareActions(object):
    def test_both_actions_empty(self):
        assert list(compare_actions({}, {})) == []

    def test_actions_1_empty(self):
        actions_2 = {
            'foo': StateAction.UPDATE,
            'cow/moo/what.txt': StateAction.DELETE,
            'what/am/I.obj': StateAction.CONFLICT
        }
        actual_output = dict(compare_actions({}, actions_2))
        expected_output = {
            'foo': SyncAction.DOWNLOAD,
            'cow/moo/what.txt': SyncAction.DELETE_LOCAL,
            'what/am/I.obj': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_actions_2_empty(self):
        actions_1 = {
            'foo': StateAction.UPDATE,
            'cow/moo/what.txt': StateAction.DELETE,
            'what/am/I.obj': StateAction.CONFLICT
        }
        actual_output = dict(compare_actions(actions_1, {}))
        expected_output = {
            'foo': SyncAction.UPLOAD,
            'cow/moo/what.txt': SyncAction.DELETE_REMOTE,
            'what/am/I.obj': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_anything_with_conflict(self):
        actions_1 = {
            'yhorm': StateAction.UPDATE,
            'ariandel': StateAction.CONFLICT,
            'artorias': StateAction.CONFLICT,
            'seath': StateAction.CONFLICT
        }
        actions_2 = {
            'yhorm': StateAction.CONFLICT,
            'artorias': StateAction.DELETE,
            'seath': StateAction.CONFLICT
        }
        actual_output = dict(compare_actions(actions_1, actions_2))
        expected_output = {
            'yhorm': SyncAction.CONFLICT,
            'ariandel': SyncAction.CONFLICT,
            'artorias': SyncAction.CONFLICT,
            'seath': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_other_conflicts(self):
        actions_1 = {
            'four/five': StateAction.DELETE,
            'six/seven': StateAction.UPDATE,
        }
        actions_2 = {
            'four/five': StateAction.UPDATE,
            'six/seven': StateAction.DELETE,
        }

        actual_output = dict(compare_actions(actions_1, actions_2))
        expected_output = {
            'four/five': SyncAction.CONFLICT,
            'six/seven': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_anything_with_none(self):
        actions_1 = {
            'foo': StateAction.UPDATE,
            'cow/moo/what.txt': StateAction.DELETE,
            'what/am/I.obj': StateAction.CONFLICT
        }
        actions_2 = {
            'foo': None,
            'cow/moo/what.txt': None,
            'what/am/I.obj': None,
        }
        actual_output = dict(compare_actions(actions_1, actions_2))
        expected_output = {
            'foo': SyncAction.UPLOAD,
            'cow/moo/what.txt': SyncAction.DELETE_REMOTE,
            'what/am/I.obj': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output
