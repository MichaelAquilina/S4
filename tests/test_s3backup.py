# -*- coding: utf-8 -*-
from s3backup import IndexAction, SyncAction, compare_actions, compare_states


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
            'apple': IndexAction.DELETE,
            'orange': IndexAction.DELETE,
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
            'foo': IndexAction.CREATE,
            'bar': IndexAction.CREATE,
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
            'red': IndexAction.UPDATE,
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
            'monkey': IndexAction.CONFLICT,
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
            'elephant': IndexAction.CREATE,
            'monkey': IndexAction.CONFLICT,
            'snake': IndexAction.DELETE,
            'dog': IndexAction.UPDATE,
        }
        assert actual_output == expected_output


class TestCompareActions(object):
    def test_both_actions_empty(self):
        assert list(compare_actions({}, {})) == []

    def test_actions_1_empty(self):
        actions_2 = {
            'foo': IndexAction.UPDATE,
            'bar': IndexAction.CREATE,
            'cow/moo/what.txt': IndexAction.DELETE,
            'what/am/I.obj': IndexAction.CONFLICT
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
            'foo': IndexAction.UPDATE,
            'bar': IndexAction.CREATE,
            'cow/moo/what.txt': IndexAction.DELETE,
            'what/am/I.obj': IndexAction.CONFLICT
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
            'yhorm': IndexAction.UPDATE,
            'ariandel': IndexAction.CONFLICT,
            'artorias': IndexAction.CONFLICT,
            'seath': IndexAction.CONFLICT
        }
        actions_2 = {
            'yhorm': IndexAction.CONFLICT,
            'ariandel': IndexAction.CREATE,
            'artorias': IndexAction.DELETE,
            'seath': IndexAction.CONFLICT
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
            'one/two/three': IndexAction.UPDATE,
            'four/five': IndexAction.DELETE,
            'six/seven': IndexAction.CREATE,
        }
        actions_2 = {
            'one/two/three': IndexAction.CREATE,
            'four/five': IndexAction.UPDATE,
            'six/seven': IndexAction.DELETE,
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
            'foo': IndexAction.UPDATE,
            'bar': IndexAction.CREATE,
            'cow/moo/what.txt': IndexAction.DELETE,
            'what/am/I.obj': IndexAction.CONFLICT
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
