# -*- coding: utf-8 -*-

import datetime
import getpass
import json
import os

import boto3


from s4.clients import local, s3


CONFIG_FOLDER_PATH = os.path.expanduser('~/.config/s4')
CONFIG_FILE_PATH = os.path.join(CONFIG_FOLDER_PATH, 'sync.conf')


def to_timestamp(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt - epoch) / datetime.timedelta(seconds=1)


def get_input(*args, secret=False, **kwargs):
    if secret:
        return getpass.getpass(*args, **kwargs)
    else:
        return input(*args, **kwargs)


def get_clients(entry):
    target_1 = entry['local_folder']
    target_2 = entry['s3_uri']
    aws_access_key_id = entry['aws_access_key_id']
    aws_secret_access_key = entry['aws_secret_access_key']
    region_name = entry['region_name']

    # append trailing slashes to prevent incorrect prefix matching on s3
    if not target_1.endswith('/'):
        target_1 += '/'
    if not target_2.endswith('/'):
        target_2 += '/'

    client_1 = get_local_client(target_1)
    client_2 = get_s3_client(target_2, aws_access_key_id, aws_secret_access_key, region_name)
    return client_1, client_2


def get_s3_client(target, aws_access_key_id, aws_secret_access_key, region_name):
    s3_uri = s3.parse_s3_uri(target)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    return s3.S3SyncClient(s3_client, s3_uri.bucket, s3_uri.key)


def get_local_client(target):
    return local.LocalSyncClient(target)


def get_config():
    if not os.path.exists(CONFIG_FILE_PATH):
        return {'targets': {}}

    with open(CONFIG_FILE_PATH, 'r') as fp:
        config = json.load(fp)
    return config


def set_config(config):
    if not os.path.exists(CONFIG_FOLDER_PATH):
        os.makedirs(CONFIG_FOLDER_PATH)

    with open(CONFIG_FILE_PATH, 'w') as fp:
        json.dump(config, fp)
