S3 Backup
=========

[![CircleCI](https://circleci.com/gh/MichaelAquilina/s3backup.svg?style=svg)](https://circleci.com/gh/MichaelAquilina/s3backup)

Fast and cheap synchronisation of files using [Amazon S3](https://aws.amazon.com/s3/).

The intention of this project is to be an open source alternative to typical proprietary sync solutions like Dropbox.
Because s3backup interacts with s3 directly, you can expect _very_ fast upload and download speeds as well as _very_
cheap costs (See [Amazon S3 Pricing](https://aws.amazon.com/s3/pricing/) for an idea of how much this would cost you).

You can also take advantage of other cool features that s3 provides like [versioning](http://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html). Everytime you sync a version of a new file,
you will now have the ability to easily rollback to any previous version.

Please note that because this project is under heavy development you should expect incomplete features, lots
of bugs and many breaking changes between commits.

Installation and Setup
----------------------
Install the the necessary requirements using pip:

```
$ pip install -r requirements.txt
```

First, run `./s3b add` to add a new sync local folder and target s3 uri:

```
$ ./s3b add
local folder: /home/username/myfolder1
s3 uri: s3://mybucket/folder1
AWS Access Key ID: AKIAJD53D9GCGKCD
AWS Secret Access Key:
region name: eu-west-2
Provide a name for this entry [myfolder1]:
```

Synchronising
-------------
Run `./s3b sync` in the project directory to synchronise the local folders you specified with the folders in the bucket.

```
$ ./s3b sync
Syncing myfolder1 [/home/username/myfolder1/ <=> s3://mybucket/folder1/]
Creating foobar.jpg (/home/username/myfolder1/ => s3://mybucket/folder1/)
Creating boarding-pass.pdf (/home/username/myfolder1/ => s3://mybucket/folder1/)
Flushing Index to Storage
```

All files will be automatically synced between the source and target destinations where possible.

You may specify a specific folder to synchronise by the name you provided during `add`.

```
$ ./s3b sync foo
```

Handling Conflicts
------------------
In the case where s3backup cannot decide on a reasonable action by itself, it will ask you to intervene:

```
Syncing /home/username/myfolder1/ with s3://mybucket/folder1/

Conflict for "test.txt". Which version would you like to keep?
   (1) /home/username/myfolder1/test.txt updated at 2017-01-23 12:26:24 (CREATED)
   (2) s3://mybucket/folder1/test.txt updated at 2017-01-23 12:26:30 (CREATED)
   (d) View difference (requires diff command)
   (3) Skip this file

Choice (default=skip):
```

If you do not wish to fix the issue, you can simply skip the file for now.

s3backup keeps track of changes between files with a `.index` file at the root of each folder you are syncing. This is
compressed (currently using gzip) to save space and increase performance when loading. Deleting this file will result
in that folder being treated as if it was never synced before so make sure you *do not* delete it unless you know what
you are doing.

Ignoring Files
--------------
Create a `.syncignore` file in the root of the directory being synced to list patterns of subdirectories and files you
wish to ignore. The `.syncignore` file uses the exact same pattern that you would expect in `.gitignore`. Each line specifies a GLOB pattern to ignore during sync.

Note that if you add a pattern which matches an item that was previously synced, that item will be deleted from the target you are syncing with next time you run s3backup.
