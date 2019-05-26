# -*- coding: utf-8 -*-

import datetime
import getpass
import gzip
import json
import os
import zlib

CONFIG_FOLDER_PATH = os.path.expanduser("~/.config/s4")
CONFIG_FILE_PATH = os.path.join(CONFIG_FOLDER_PATH, "sync.conf")


def try_decompress(body):
    # Attempt multiple levels of fallback in case we cannot
    # figure out the compression type using `magic`
    try:
        return gzip.decompress(body)
    except OSError:
        pass

    try:
        return zlib.decompress(body)
    except zlib.error:
        raise ValueError("Unknown compression format")


def to_timestamp(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt - epoch) / datetime.timedelta(seconds=1)


def get_input(*args, secret=False, required=False, blank=False, **kwargs):
    """
    secret: Don't show user input when they are typing.
    required: Keep prompting if the user enters an empty value.
    blank: turn all empty strings into None.
    """

    while True:
        if secret:
            value = getpass.getpass(*args, **kwargs)
        else:
            value = input(*args, **kwargs)

        if blank:
            value = value if value else None

        if not required or value:
            break

    return value


def get_config():
    if not os.path.exists(CONFIG_FILE_PATH):
        return {"targets": {}}

    with open(CONFIG_FILE_PATH, "r") as fp:
        config = json.load(fp)
    return config


def set_config(config):
    if not os.path.exists(CONFIG_FOLDER_PATH):
        os.makedirs(CONFIG_FOLDER_PATH)

    with open(CONFIG_FILE_PATH, "w") as fp:
        json.dump(config, fp, indent=5)
