#! -*- encoding: utf8 -*-

try:
    from os import scandir
except ImportError:
    from scandir import scandir

from inotify_simple import INotify


class INotifyRecursive(INotify):
    def add_watches(self, path, mask):
        results = {}
        results[self.add_watch(path, mask)] = path

        for item in scandir(path):
            if item.is_dir():
                results.update(self.add_watches(item.path, mask))

        return results
