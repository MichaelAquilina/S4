# -*- coding: utf-8 -*-
from s3backup import IndexAction, compare_states


class TestCompareStates(object):
    def test_both_empty(self):
        assert list(compare_states({}, {})) == []

    def test_empty_current(self):
        current = {}
        previous = {
            'orange': dict(timestamp=99999),
            'apple': dict(timestamp=88888),
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('apple', IndexAction.DELETE),
            ('orange', IndexAction.DELETE),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_empty_previous(self):
        current = {
            'foo': dict(timestamp=400123),
            'bar': dict(timestamp=23231),
        }
        previous = {}

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('foo', IndexAction.CREATE),
            ('bar', IndexAction.CREATE),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_current(self):
        current = {
            'red': dict(timestamp=1234567),
        }
        previous = {
            'red': dict(timestamp=1000000),
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('red', IndexAction.UPDATE),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_previous(self):
        current = {
            'monkey': dict(timestamp=8000),
        }
        previous = {
            'monkey': dict(timestamp=1000000),
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('monkey', IndexAction.CONFLICT),
        ]
        assert sorted(actual_output) == sorted(expected_output)

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

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('elephant', IndexAction.CREATE),
            ('monkey', IndexAction.CONFLICT),
            ('snake', IndexAction.DELETE),
            ('dog', IndexAction.UPDATE),
        ]
        assert sorted(actual_output) == sorted(expected_output)
