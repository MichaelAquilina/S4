S3 Backup
=========

[![CircleCI](https://circleci.com/gh/MichaelAquilina/s3backup.svg?style=svg)](https://circleci.com/gh/MichaelAquilina/s3backup)

WIP synchronisation of local files with s3 for backup.

Setup
-----

First, [configure boto3 to use appropriate aws credentials](https://boto3.readthedocs.io/en/latest/guide/configuration.html).

Create a `.s3syncrc` file in your home direcoty with the following JSON structure:

```
{
  "bucket": "s3://mybucketurl",
  "directories": {
    "/home/username/myfolder1": "myfolder1",
    "/home/username/myfolder2": "myfolder2",
  }
}
```

Run `./sync` in the project directory to synchronise the local folders you specified with the folders in the bucket.
