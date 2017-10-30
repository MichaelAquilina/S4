#! -*- encoding: utf-8 -*-

import tqdm


class ProgressBar(object):
    """
    Singleton wrapper around tqdm
    """
    pbar = None

    def __new__(cls, *args, **kwargs):
        if cls.pbar:
            cls.pbar.close()

        cls.pbar = tqdm.tqdm(*args, **kwargs)

    @classmethod
    def update(cls, value):
        cls.pbar.update(value)

    @classmethod
    def close(cls):
        cls.pbar.close()
