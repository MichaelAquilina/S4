#! -*- encoding: utf8 -*-
from inotify_simple import flags

import pytest

from s4.inotify_recursive import INotifyRecursive


class TestINotifyRecursive(object):
    @pytest.mark.timeout(5)
    def test_add_watches(self, tmpdir):
        foo = tmpdir.mkdir("foo")
        bar = tmpdir.mkdir("bar")
        baz = bar.mkdir("baz")

        notifier = INotifyRecursive()
        result_1 = notifier.add_watches(str(foo), flags.CREATE)
        result_2 = notifier.add_watches(str(bar), flags.CREATE)

        assert sorted(result_1.values()) == sorted([str(foo)])
        assert sorted(result_2.values()) == sorted([str(bar), str(baz)])

        bar.join("hello.txt").write("hello")
        foo.join("fennek.md").write("*jumps*")
        baz.mkdir("bong")

        events = notifier.read()
        assert len(events) == 3

        assert events[0].name == 'hello.txt'
        assert result_2[events[0].wd] == str(bar)

        assert events[1].name == 'fennek.md'
        assert result_1[events[1].wd] == str(foo)

        assert events[2].name == 'bong'
        assert result_2[events[2].wd] == str(baz)
