# -*- coding: utf-8 -*-

import datetime
import getpass
import json
import os


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
