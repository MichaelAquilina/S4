# -*- coding: utf-8 -*-
import datetime
import os

import freezegun


def touch_local(client, key, timestamp=None, data=''):
    if timestamp is None:
        times = None
    else:
        times = (timestamp, timestamp)

    target_path = os.path.join(client.path, key)
    parent = os.path.dirname(target_path)
    if not os.path.exists(parent):
        os.makedirs(parent)

    with open(target_path, 'w') as fp:
        fp.write(data)

    with open(target_path, 'w'):
        os.utime(client.path, times)


def touch_s3(client, key, timestamp=None, data=''):
    if timestamp is None:
        last_modified = datetime.datetime.utcnow()
    else:
        last_modified = datetime.datetime.utcfromtimestamp(timestamp)

    with freezegun.freeze_time(last_modified):
        client.client.put_object(
            Bucket=client.bucket,
            Key=os.path.join(client.prefix, key),
            Body=data,
        )
