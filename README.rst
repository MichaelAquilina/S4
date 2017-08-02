==============
S4 = S3 Syncer
==============

|TravisCI| |CodeCov| |PyPi| |GPLv3| |ReadTheDocs|

Fast and cheap synchronisation of files using `Amazon
S3 <https://aws.amazon.com/s3/>`__.

S4 stands for "Simple Storage Solution (S3) Syncer".

The intention of this project is to be an open source alternative to
typical proprietary sync solutions like Dropbox. Because s4 interacts
with S3 directly, you can expect *very* fast upload and download speeds
as well as *very* cheap costs (See `Amazon S3
Pricing <https://aws.amazon.com/s3/pricing/>`__ for an idea of how much
this would cost you). See `Why?`_ for further motivation for this project.

You can also take advantage of other cool features that S3 provides like
`versioning <http://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html>`__.
Everytime you sync a version of a new file, you will now have the
ability to easily rollback to any previous version.

See it in action here:

|ASCIINEMA|

Requirements
------------

S4 requires python 3.3+ to work

Installation
------------

The easiest way to install S4 is through pip:

::

    $ pip install s4


Setup
-----

Run ``s4 add`` to add a new sync local folder and target S3 uri:

::

    $ s4 add
    local folder: /home/username/myfolder1
    s3 uri: s3://mybucket/folder1
    AWS Access Key ID: AKIAJD53D9GCGKCD
    AWS Secret Access Key:
    region name: eu-west-2
    Provide a name for this entry [myfolder1]:

Synchronising
-------------

Run ``s4 sync`` in the project directory to synchronise the local
folders you specified with the folders in the bucket.

::

    $ s4 sync
    Syncing myfolder1 [/home/username/myfolder1/ <=> s3://mybucket/folder1/]
    Creating foobar.jpg (/home/username/myfolder1/ => s3://mybucket/folder1/)
    Creating boarding-pass.pdf (/home/username/myfolder1/ => s3://mybucket/folder1/)
    Flushing Index to Storage

All files will be automatically synced between the source and target
destinations where possible.

You may specify a specific folder to synchronise by the name you
provided during ``add``.

::

    $ s4 sync myfolder1

Handling Conflicts
------------------

In the case where S4 cannot decide on a reasonable action by itself, it
will ask you to intervene:

::

    Syncing /home/username/myfolder1/ with s3://mybucket/folder1/

    Conflict for "test.txt". Which version would you like to keep?
       (1) /home/username/myfolder1/test.txt updated at 2017-01-23 12:26:24 (CREATED)
       (2) s3://mybucket/folder1/test.txt updated at 2017-01-23 12:26:30 (CREATED)
       (d) View difference (requires diff command)
       (3) Skip this file

    Choice (default=skip):

If you do not wish to fix the issue, you can simply skip the file for
now.

Other Subommands
----------------

Some other subcommands that you could find useful:

-  ``s4 targets`` - print existing targets
-  ``s4 edit`` - edit the settings of a targets
-  ``s4 rm`` - remove a target
-  ``s4 ls`` - print tracked files and metadata of a target

Use the ``--help`` parameter on each subcommand to get more details.

How S4 Works
-------------

S4 keeps track of changes between files with a ``.index`` file at
the root of each folder you are syncing. This contains the keys of each
file being synchronised along with the timestamps found locally and
remotely in JSON format.

This is compressed (currently using gzip) to save space and increase
performance when loading.

If you are curious, you can view the contents of an index file using the
`s4 ls` subcommand or you can view the file directly using a command
like `zcat`.

    NOTE: Deleting this file will result in that folder being treated as if
    it was never synced before so make sure you *do not* delete it unless
    you know what you are doing.

Ignoring Files
--------------

Create a ``.syncignore`` file in the root of the directory being synced
to list patterns of subdirectories and files you wish to ignore. The
``.syncignore`` file uses the exact same pattern that you would expect
in ``.gitignore``. Each line specifies a `GLOB
pattern <https://en.wikipedia.org/wiki/Glob_%28programming%29>`__ to
ignore during sync.

Note that if you add a pattern which matches an item that was previously
synced, that item will be deleted from the target you are syncing with
next time you run S4.

Why?
----

There are a number of open source S3 backup tools out there - but none of them really satisfied the
requirements that this project tries to solve.

Here are is a list of open source solutions that I have tried in the past.

* ``s3cmd``: Provides a sync function that works very well for backing up - but stops working correctly
  as soon as there is second machine you want to sync to S3.

* ``owncloud/nextcloud``: Requires you to setup a server to perform your syncing. In terms of costs on AWS,
  this quickly becomes costly compared with just using S3. The speed of your uploads and downloads are also
  heavily bottlenectked by the network and hardware performance of your ec2 instance.

* ``seafile``: suffers from the same problem as owncloud/nextcloud.

* ``duplicity``: great backup tool, but does not provide a sync solution of any kind.

.. |TravisCI| image:: https://travis-ci.org/MichaelAquilina/S4.svg?branch=master
   :target: https://travis-ci.org/MichaelAquilina/S4

.. |PyPi| image:: https://badge.fury.io/py/s4.svg
   :target: https://badge.fury.io/py/s4

.. |CodeCov| image:: https://codecov.io/gh/MichaelAquilina/s4/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/MichaelAquilina/s4

.. |GPLv3| image:: https://img.shields.io/badge/License-GPL%20v3-blue.svg
   :target: https://www.gnu.org/licenses/gpl-3.0

.. |ReadTheDocs| image:: https://readthedocs.org/projects/s4docs/badge/?version=latest
   :target: https://s4docs.readthedocs.org

.. |ASCIINEMA| image:: https://asciinema.org/a/0GiXLN7YT4ki8qouedF0w8Wbk.png
   :target: https://asciinema.org/a/0GiXLN7YT4ki8qouedF0w8Wbk?autoplay=1
