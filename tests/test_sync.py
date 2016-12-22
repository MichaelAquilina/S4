# -*- coding: utf-8 -*-
from s3backup import IndexAction, compare_states
from s3backup.clients.entries import FileEntry


class TestFileEntry(object):
    def test_repr(self):
        entry = FileEntry('foo/bar/baz.txt', 231230.4)
        assert repr(entry) == 'FileEntry(foo/bar/baz.txt, 231230.4)'


class TestCompareStates(object):
    def test_both_empty(self):
        assert list(compare_states({}, {})) == []

    def test_empty_current(self):
        current = {}
        previous = {
            'orange': FileEntry('orange', 99999),
            'apple': FileEntry('apple', 88888),
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('apple', IndexAction.DELETE),
            ('orange', IndexAction.DELETE),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_empty_previous(self):
        current = {
            'foo': FileEntry('foo', 400123),
            'bar': FileEntry('bar', 23231),
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
            'red': FileEntry('red', 1234567),
        }
        previous = {
            'red': FileEntry('red', 1000000),
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('red', IndexAction.UPDATE),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_new_previous(self):
        current = {
            'monkey': FileEntry('monkey', 8000),
        }
        previous = {
            'monkey': FileEntry('monkey', 1000000),
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('monkey', IndexAction.CONFLICT),
        ]
        assert sorted(actual_output) == sorted(expected_output)

    def test_mixed(self):
        current = {
            'monkey': FileEntry('monkey', 8000),
            'elephant': FileEntry('elephant', 3232323),
            'dog': FileEntry('dog', 23233232323),
        }
        previous = {
            'monkey': FileEntry('monkey', 1000000),
            'snake': FileEntry('snake', 232323),
            'dog': FileEntry('dog', 2333)
        }

        actual_output = list(compare_states(current, previous))
        expected_output = [
            ('elephant', IndexAction.CREATE),
            ('monkey', IndexAction.CONFLICT),
            ('snake', IndexAction.DELETE),
            ('dog', IndexAction.UPDATE),
        ]
        assert sorted(actual_output) == sorted(expected_output)
