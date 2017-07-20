# -*- coding: utf-8 -*-

import datetime
import getpass


def to_timestamp(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt - epoch) / datetime.timedelta(seconds=1)


def get_input(*args, secret=False, **kwargs):
    if secret:
        return getpass.getpass(*args, **kwargs)
    else:
        return input(*args, **kwargs)
