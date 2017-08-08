# -*- coding: utf-8 -*-

import argparse
import datetime
import json
import logging
import os
import sys
from collections import defaultdict

import boto3

from inotify_simple import INotify, flags

try:
    from os import scandir
except ImportError:
    from scandir import scandir

from tabulate import tabulate

from s4 import VERSION
from s4 import sync
from s4 import utils
from s4.clients import local, s3


CONFIG_FOLDER_PATH = os.path.expanduser('~/.config/s4')
CONFIG_FILE_PATH = os.path.join(CONFIG_FOLDER_PATH, 'sync.conf')


def get_s3_client(target, aws_access_key_id, aws_secret_access_key, region_name):
    s3_uri = s3.parse_s3_uri(target)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    return s3.S3SyncClient(s3_client, s3_uri.bucket, s3_uri.key)


def get_local_client(target):
    return local.LocalSyncClient(target)


def main(arguments):
    parser = argparse.ArgumentParser(
        prog='s4',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            'Fast and cheap synchronisation of files with Amazon S3\n'
            '\n'
            'Version: {}\n'
            '\n'
            'To start off, add a Target with the "add" command\n'
        ).format(VERSION),
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
    )
    parser.add_argument(
        '--timestamps',
        action='store_true',
        help='Display timestamps for each log message',
    )
    subparsers = parser.add_subparsers(dest='command')

    daemon_parser = subparsers.add_parser('daemon', help="Run S4 continiously")
    daemon_parser.add_argument('targets', nargs='*')
    daemon_parser.add_argument('--read-delay', default=1000, type=int)
    daemon_parser.add_argument('--conflicts', default='ignore', choices=['1', '2', 'ignore'])

    subparsers.add_parser('add', help="Add a new Target to synchronise")

    sync_parser = subparsers.add_parser('sync', help="Synchronise Targets with S3")
    sync_parser.add_argument('targets', nargs='*')
    sync_parser.add_argument('--conflicts', default=None, choices=['1', '2', 'ignore'])

    edit_parser = subparsers.add_parser('edit', help="Edit Target details")
    edit_parser.add_argument('target')

    subparsers.add_parser('targets', help="Print available Targets")

    subparsers.add_parser('version', help="Print S4 Version")

    ls_parser = subparsers.add_parser('ls', help="Display list of files for a Target")
    ls_parser.add_argument('target')
    ls_parser.add_argument('--sort-by', '-s', choices=['key', 'local', 's3'], default='key')
    ls_parser.add_argument('--descending', '-d', action='store_true')
    ls_parser.add_argument(
        '--all', '-A',
        dest='show_all',
        action='store_true',
        help='show deleted files',
    )

    remove_parser = subparsers.add_parser('rm', help="Remove a Target")
    remove_parser.add_argument('target')

    args = parser.parse_args(arguments)

    if args.log_level == 'DEBUG':
        log_format = '%(levelname)s:%(module)s:%(lineno)s %(message)s'
    else:
        log_format = '%(message)s'

    if args.timestamps:
        log_format = '%(asctime)s: ' + log_format

    logging.basicConfig(format=log_format, level=args.log_level)

    # shut boto up
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('nose').setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

    logger = logging.getLogger(__name__)
    logger.setLevel(args.log_level)

    config = get_config()

    try:
        if args.command == 'version':
            print(VERSION)
        elif args.command == 'sync':
            sync_command(args, config, logger)
        elif args.command == 'targets':
            targets_command(args, config, logger)
        elif args.command == 'add':
            add_command(args, config, logger)
        elif args.command == 'edit':
            edit_command(args, config, logger)
        elif args.command == 'ls':
            ls_command(args, config, logger)
        elif args.command == 'rm':
            rm_command(args, config, logger)
        elif args.command == 'daemon':
            daemon_command(args, config, logger)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        pass


def get_config():
    if not os.path.exists(CONFIG_FILE_PATH):
        return {}

    with open(CONFIG_FILE_PATH, 'r') as fp:
        config = json.load(fp)
    return config


def set_config(config):
    if not os.path.exists(CONFIG_FOLDER_PATH):
        os.makedirs(CONFIG_FOLDER_PATH)

    with open(CONFIG_FILE_PATH, 'w') as fp:
        json.dump(config, fp, indent=4)


def get_clients(entry):
    target_1 = entry['local_folder']
    target_2 = entry['s3_uri']
    aws_access_key_id = entry['aws_access_key_id']
    aws_secret_access_key = entry['aws_secret_access_key']
    region_name = entry['region_name']

    # append trailing slashes to prevent incorrect prefix matching on s3
    if not target_1.endswith('/'):
        target_1 += '/'
    if not target_2.endswith('/'):
        target_2 += '/'

    client_1 = get_local_client(target_1)
    client_2 = get_s3_client(target_2, aws_access_key_id, aws_secret_access_key, region_name)
    return client_1, client_2


def get_sync_worker(entry):
    client_1, client_2 = get_clients(entry)
    return sync.SyncWorker(client_1, client_2)


class INotifyRecursive(INotify):
    def add_watches(self, path, mask):
        results = {}
        results[self.add_watch(path, mask)] = path

        for item in scandir(path):
            if item.is_dir():
                results.update(self.add_watches(item.path, mask))

        return results


def daemon_command(args, config, logger):
    all_targets = list(config['targets'].keys())
    if not args.targets:
        targets = all_targets
    else:
        targets = args.targets

    notifier = INotifyRecursive()
    watch_flags = flags.CREATE | flags.DELETE | flags.MODIFY

    watch_map = {}

    for target in targets:
        entry = config['targets'][target]
        path = entry['local_folder']
        logger.info("Watching %s", path)
        for wd in notifier.add_watches(path.encode('utf8'), watch_flags):
            watch_map[wd] = target

        # Check for any pending changes
        worker = get_sync_worker(entry)
        worker.sync(conflict_choice=args.conflicts)

    while True:
        to_run = defaultdict(set)
        for event in notifier.read(read_delay=args.read_delay):
            target = watch_map[event.wd]

            # Dont bother running for .index
            if event.name != '.index':
                to_run[target].add(event.name)

        for target, keys in to_run.items():
            entry = config['targets'][target]
            worker = get_sync_worker(entry)

            # Should ideally be setting keys to sync
            logger.info('Syncing {}'.format(worker))
            worker.sync(conflict_choice=args.conflicts)


def sync_command(args, config, logger):
    all_targets = list(config['targets'].keys())
    if not args.targets:
        targets = all_targets
    else:
        targets = args.targets

    try:
        for name in sorted(targets):
            if name not in config['targets']:
                logger.info('"%s" is an unknown target. Choices are: %s', name, all_targets)
                continue

            entry = config['targets'][name]
            client_1, client_2 = get_clients(entry)

            try:
                worker = sync.SyncWorker(client_1, client_2)

                logger.info('Syncing %s [%s <=> %s]', name, client_1.get_uri(), client_2.get_uri())
                worker.sync(conflict_choice=args.conflicts)
            except Exception as e:
                logger.error("There was an error syncing '%s': %s", name, e)
    except KeyboardInterrupt:
        logger.warning('Quitting due to Keyboard Interrupt...')


def targets_command(args, config, logger):
    if 'targets' not in config:
        return

    for name in sorted(config['targets']):
        entry = config['targets'][name]
        logger.info('%s: [%s <=> %s]', name, entry['local_folder'], entry['s3_uri'])


def add_command(args, config, logger):
    entry = {}

    entry['local_folder'] = os.path.expanduser(utils.get_input('local folder: '))
    entry['s3_uri'] = utils.get_input('s3 uri: ')
    entry['aws_access_key_id'] = utils.get_input('AWS Access Key ID: ')
    entry['aws_secret_access_key'] = utils.get_input('AWS Secret Access Key: ', secret=True)
    entry['region_name'] = utils.get_input('region name: ')

    default_name = os.path.basename(entry['s3_uri'])
    name = utils.get_input('Provide a name for this entry [{}]: '.format(default_name))

    if not name:
        name = default_name

    if 'targets' not in config:
        config['targets'] = {}

    config['targets'][name] = entry

    set_config(config)


def edit_command(args, config, logger):
    if 'targets' not in config:
        logger.info('You have not added any targets yet')
        logger.info('Use the "add" command to do this')
        return

    if args.target not in config['targets']:
        all_targets = sorted(list(config['targets'].keys()))
        logger.info('"%s" is an unknown target', args.target)
        logger.info('Choices are: %s', all_targets)
        return

    entry = config['targets'][args.target]

    local_folder = entry.get('local_folder', '')
    s3_uri = entry.get('s3_uri', '')
    aws_access_key_id = entry.get('aws_access_key_id')
    aws_secret_access_key = entry.get('aws_secret_access_key')
    region_name = entry.get('region_name')

    new_local_folder = utils.get_input('local folder [{}]: '.format(local_folder))
    new_s3_uri = utils.get_input('s3 uri [{}]: '.format(s3_uri))
    new_aws_access_key_id = utils.get_input('AWS Access Key ID [{}]: '.format(aws_access_key_id))

    secret_key_prompt = 'AWS Secret Access Key [{}]: '.format(aws_secret_access_key)
    new_aws_secret_access_key = utils.get_input(secret_key_prompt, secret=True)

    new_region_name = utils.get_input('region name [{}]: '.format(region_name))

    if new_local_folder:
        entry['local_folder'] = new_local_folder
    if new_s3_uri:
        entry['s3_uri'] = new_s3_uri
    if new_aws_access_key_id:
        entry['aws_access_key_id'] = new_aws_access_key_id
    if new_aws_secret_access_key:
        entry['aws_secret_access_key'] = new_aws_secret_access_key
    if new_region_name:
        entry['region_name'] = new_region_name

    config['targets'][args.target] = entry

    set_config(config)


def ls_command(args, config, logger):
    if 'targets' not in config:
        logger.info('You have not added any targets yet')
        logger.info('Use the "add" command to do this')
        return
    if args.target not in config['targets']:
        all_targets = sorted(list(config['targets'].keys()))
        logger.info('"%s" is an unknown target', args.target)
        logger.info('Choices are: %s', all_targets)
        return

    target = config['targets'][args.target]
    client_1, client_2 = get_clients(target)

    sort_by = args.sort_by.lower()
    descending = args.descending

    keys = set(client_1.index) | set(client_2.index)

    data = []
    for key in sorted(keys):
        entry_1 = client_1.index.get(key, {})
        entry_2 = client_2.index.get(key, {})

        ts_1 = entry_1.get('local_timestamp')
        ts_2 = entry_2.get('local_timestamp')

        if args.show_all or ts_1 is not None:
            data.append((
                key,
                datetime.datetime.utcfromtimestamp(int(ts_1)) if ts_1 is not None else '<deleted>',
                datetime.datetime.utcfromtimestamp(int(ts_2)) if ts_2 is not None else None,
            ))

    headers = ['key', 'local', 's3']
    data = sorted(data, reverse=descending, key=lambda x: x[headers.index(sort_by)])

    logger.info(tabulate(data, headers=headers))


def rm_command(args, config, logger):
    if 'targets' not in config:
        logger.info('You have not added any targets yet')
        return
    if args.target not in config['targets']:
        all_targets = sorted(list(config['targets'].keys()))
        logger.info('"%s" is an unknown target', args.target)
        logger.info('Choices are: %s', all_targets)
        return

    del config['targets'][args.target]
    set_config(config)


if __name__ == '__main__':
    main(sys.argv[1:])
