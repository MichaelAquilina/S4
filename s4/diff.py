#! -*- encoding: utf8 -*-

import os
import shutil
import subprocess
import tempfile


def show_diff(client_1, client_2, key):
    if shutil.which("diff") is None:
        print('Missing required "diff" executable.')
        print("Install this using your distribution's package manager")
        return

    if shutil.which("less") is None:
        print('Missing required "less" executable.')
        print("Install this using your distribution's package manager")
        return

    so_1 = client_1.get(key)
    data_1 = so_1.fp.read()
    so_1.fp.close()

    so_2 = client_2.get(key)
    data_2 = so_2.fp.read()
    so_2.fp.close()

    fd_1, path_1 = tempfile.mkstemp()
    fd_2, path_2 = tempfile.mkstemp()
    fd_3, path_3 = tempfile.mkstemp()

    with open(path_1, "wb") as fp:
        fp.write(data_1)
    with open(path_2, "wb") as fp:
        fp.write(data_2)

    # This is a lot faster than the difflib found in python
    with open(path_3, "wb") as fp:
        subprocess.call(
            [
                "diff",
                "-u",
                "--label",
                client_1.get_uri(key),
                path_1,
                "--label",
                client_2.get_uri(key),
                path_2,
            ],
            stdout=fp,
        )

    subprocess.call(["less", path_3])

    os.close(fd_1)
    os.close(fd_2)
    os.close(fd_3)

    os.remove(path_1)
    os.remove(path_2)
    os.remove(path_3)
