# -*- coding: utf-8 -*-
from s3backup import StateAction, SyncAction, compare_actions, compare_states


class TestCompareStates(object):
    def test_both_empty(self):
        assert list(compare_states({}, {})) == []

    def test_empty_current(self):
        current = {}
        previous = {
            'orange': dict(timestamp=99999),
            'apple': dict(timestamp=88888),
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'apple': StateAction.DELETE,
            'orange': StateAction.DELETE,
        }
        assert actual_output == expected_output

    def test_empty_previous(self):
        current = {
            'foo': dict(timestamp=400123),
            'bar': dict(timestamp=23231),
        }
        previous = {}

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'foo': StateAction.CREATE,
            'bar': StateAction.CREATE,
        }
        assert actual_output == expected_output

    def test_new_current(self):
        current = {
            'red': dict(timestamp=1234567),
        }
        previous = {
            'red': dict(timestamp=1000000),
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'red': StateAction.UPDATE,
        }
        assert actual_output == expected_output

    def test_new_previous(self):
        current = {
            'monkey': dict(timestamp=8000),
        }
        previous = {
            'monkey': dict(timestamp=1000000),
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'monkey': StateAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_mixed(self):
        current = {
            'monkey': dict(timestamp=8000),
            'elephant': dict(timestamp=3232323),
            'dog': dict(timestamp=23233232323),
        }
        previous = {
            'monkey': dict(timestamp=1000000),
            'snake': dict(timestamp=232323),
            'dog': dict(timestamp=2333)
        }

        actual_output = dict(compare_states(current, previous))
        expected_output = {
            'elephant': StateAction.CREATE,
            'monkey': StateAction.CONFLICT,
            'snake': StateAction.DELETE,
            'dog': StateAction.UPDATE,
        }
        assert actual_output == expected_output


class TestCompareActions(object):
    def test_both_actions_empty(self):
        assert list(compare_actions({}, {})) == []

    def test_actions_1_empty(self):
        actions_2 = {
            'foo': StateAction.UPDATE,
            'bar': StateAction.CREATE,
            'cow/moo/what.txt': StateAction.DELETE,
            'what/am/I.obj': StateAction.CONFLICT
        }
        actual_output = dict(compare_actions({}, actions_2))
        expected_output = {
            'foo': SyncAction.DOWNLOAD,
            'bar': SyncAction.DOWNLOAD,
            'cow/moo/what.txt': SyncAction.DELETE_LOCAL,
            'what/am/I.obj': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_actions_2_empty(self):
        actions_1 = {
            'foo': StateAction.UPDATE,
            'bar': StateAction.CREATE,
            'cow/moo/what.txt': StateAction.DELETE,
            'what/am/I.obj': StateAction.CONFLICT
        }
        actual_output = dict(compare_actions(actions_1, {}))
        expected_output = {
            'foo': SyncAction.UPLOAD,
            'bar': SyncAction.UPLOAD,
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
            'ariandel': StateAction.CREATE,
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

    def test_other_conficts(self):
        actions_1 = {
            'one/two/three': StateAction.UPDATE,
            'four/five': StateAction.DELETE,
            'six/seven': StateAction.CREATE,
        }
        actions_2 = {
            'one/two/three': StateAction.CREATE,
            'four/five': StateAction.UPDATE,
            'six/seven': StateAction.DELETE,
        }

        actual_output = dict(compare_actions(actions_1, actions_2))
        expected_output = {
            'one/two/three': SyncAction.CONFLICT,
            'four/five': SyncAction.CONFLICT,
            'six/seven': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output

    def test_anything_with_none(self):
        actions_1 = {
            'foo': StateAction.UPDATE,
            'bar': StateAction.CREATE,
            'cow/moo/what.txt': StateAction.DELETE,
            'what/am/I.obj': StateAction.CONFLICT
        }
        actions_2 = {
            'foo': None,
            'bar': None,
            'cow/moo/what.txt': None,
            'what/am/I.obj': None,
        }
        actual_output = dict(compare_actions(actions_1, actions_2))
        expected_output = {
            'foo': SyncAction.UPLOAD,
            'bar': SyncAction.UPLOAD,
            'cow/moo/what.txt': SyncAction.DELETE_REMOTE,
            'what/am/I.obj': SyncAction.CONFLICT,
        }
        assert actual_output == expected_output
