# -*- coding: utf-8 -*-

import os


class LocalSyncClient(object):
    def __init__(self, local_dir):
        self.local_dir = local_dir

    def get_object_timestamp(self, key):
        object_path = os.path.join(self.local_dir, key)
        if os.path.exists(object_path):
            return os.stat(object_path).st_mtime
        else:
            return None

    def update_sync_index(self):
        pass

    def keys(self):
        for item in os.listdir(self.local_dir):
            if item.startswith('.'):
                continue

            if os.path.isfile(os.path.join(self.local_dir, item)):
                yield item

    def put_object(self, key, fp, timestamp):
        object_path = os.path.join(self.local_dir, key)

        object_stat = None
        if os.path.exists(object_path):
            object_stat = os.stat(object_path)

        with open(object_path, 'wb') as fp2:
            fp2.write(fp.read())

        if object_stat is not None:
            os.utime(object_path, (object_stat.st_atime, timestamp))

    def get_object(self, key):
        return open(os.path.join(self.local_dir, key), 'rb')
