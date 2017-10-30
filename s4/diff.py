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

    so1 = client_1.get(key)
    data1 = so1.fp.read()
    so1.fp.close()

    so2 = client_2.get(key)
    data2 = so2.fp.read()
    so2.fp.close()

    fd1, path1 = tempfile.mkstemp()
    fd2, path2 = tempfile.mkstemp()
    fd3, path3 = tempfile.mkstemp()

    with open(path1, 'wb') as fp:
        fp.write(data1)
    with open(path2, 'wb') as fp:
        fp.write(data2)

    # This is a lot faster than the difflib found in python
    with open(path3, 'wb') as fp:
        subprocess.call([
            'diff', '-u',
            '--label', client_1.get_uri(key), path1,
            '--label', client_2.get_uri(key), path2,
        ], stdout=fp)

    subprocess.call(['less', path3])

    os.close(fd1)
    os.close(fd2)
    os.close(fd3)

    os.remove(path1)
    os.remove(path2)
    os.remove(path3)
