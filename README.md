S3 Backup
=========

[![CircleCI](https://circleci.com/gh/MichaelAquilina/s3backup.svg?style=svg)](https://circleci.com/gh/MichaelAquilina/s3backup)

WIP synchronisation of local files with S3 for backup.

Setup
-----

First, [configure boto3 to use appropriate aws credentials](https://boto3.readthedocs.io/en/latest/guide/configuration.html).

Create a `sync.conf` file in your `~/.config/s3backup` directory with the following JSON structure:

```json
{
  "directories": [
    {
      "source": "/home/username/myfolder1",
      "target": "s3://mybucket/myfolder1"
    },
    {
      "source": "/home/username/myfolder2",
      "target": "s3://mybucket/myfolder2"
    }
  ]
}
```

Run `./sync` in the project directory to synchronise the local folders you specified with the folders in the bucket.
