# -*- coding: utf-8 -*-
import datetime
import json
import os

import freezegun


class InterruptedBytesIO(object):
    """
    Test helper class that imitates a BytesIO stream. Will return a stream of 0s for
    every requested read. When the number of times read was read reaches the parameter
    value of `interrupt_at`, a ValueError will be raised to simulate an interrupted IO
    transfer that could have occured
    """
    def __init__(self, interrupt_at=1):
        self.index = 0
        self.interrupt_at = interrupt_at

    def read(self, size):
        if self.index == self.interrupt_at:
            raise ValueError('Interrupted IO')
        else:
            self.index += 1
            return b'0' * size


def write_local(path, data=''):
    parent = os.path.dirname(path)
    if not os.path.exists(parent):
        os.makedirs(parent)
    with open(path, 'w') as fp:
        fp.write(data)


def get_local_contents(client, key):
    with open(os.path.join(client.path, key), 'rb') as fp:
        return fp.read()


def set_local_contents(client, key, timestamp=None, data=''):
    path = os.path.join(client.path, key)
    write_local(path, data)
    if timestamp is not None:
        os.utime(path, (timestamp, timestamp))


def delete_local(client, key):
    os.remove(os.path.join(client.path, key))


def set_local_index(client, data):
    with open(client.index_path(), 'w') as fp:
        json.dump(data, fp)
    client.reload_index()


def write_s3(boto, bucket, key, data=''):
    boto.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
    )


def set_s3_contents(s3_client, key, timestamp=None, data=''):
    if timestamp is None:
        freeze_time = datetime.datetime.utcnow()
    else:
        freeze_time = datetime.datetime.utcfromtimestamp(timestamp)

    with freezegun.freeze_time(freeze_time):
        write_s3(s3_client.boto, s3_client.bucket, os.path.join(s3_client.prefix, key), data)


def set_s3_index(s3_client, data):
    s3_client.boto.put_object(
        Bucket=s3_client.bucket,
        Key=os.path.join(s3_client.prefix, '.index'),
        Body=json.dumps(data),
    )
    s3_client.reload_index()
