# -*- coding: utf-8 -*-
import datetime
import os

import freezegun


def write_local(path, data=''):
    parent = os.path.dirname(path)
    if not os.path.exists(parent):
        os.makedirs(parent)
    with open(path, 'w') as fp:
        fp.write(data)


def set_local_contents(client, key, timestamp=None, data=''):
    path = os.path.join(client.path, key)
    write_local(path, data)
    if timestamp is not None:
        os.utime(path, (timestamp, timestamp))


def set_s3_contents(s3_client, key, timestamp=None, data=''):
    if timestamp is None:
        freeze_time = datetime.datetime.utcnow()
    else:
        freeze_time = datetime.datetime.utcfromtimestamp(timestamp)

    with freezegun.freeze_time(freeze_time):
        s3_client.client.put_object(
            Bucket=s3_client.bucket,
            Key=os.path.join(s3_client.prefix, key),
            Body=data,
        )
