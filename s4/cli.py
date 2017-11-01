# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
from collections import defaultdict

from inotify_simple import flags

from s4 import VERSION
from s4 import sync
from s4 import utils
from s4.clients.local import get_local_client
from s4.clients.s3 import get_s3_client
from s4.commands.add_command import AddCommand
from s4.commands.ls_command import LsCommand
from s4.commands.sync_command import SyncCommand
from s4.commands.targets_command import TargetsCommand
from s4.inotify_recursive import INotifyRecursive


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
        '--no-colors',
        action='store_true',
        help='Display without colors',
    )
    parser.add_argument(
        '--timestamps',
        action='store_true',
        help='Display timestamps for each log message',
    )
    subparsers = parser.add_subparsers(dest='command')

    daemon_parser = subparsers.add_parser('daemon', help="Run S4 sync continiously")
    daemon_parser.add_argument('targets', nargs='*')
    daemon_parser.add_argument('--read-delay', default=1000, type=int)
    daemon_parser.add_argument('--conflicts', default='ignore', choices=['1', '2', 'ignore'])

    add_parser = subparsers.add_parser('add', help="Add a new Target to synchronise")
    add_parser.add_argument(
        '--copy-target-credentials',
        '-C',
        help="Copy credentials from an existing target instead of typing them in again"
    )

    sync_parser = subparsers.add_parser('sync', help="Synchronise Targets with S3")
    sync_parser.add_argument('targets', nargs='*')
    sync_parser.add_argument('--conflicts', default=None, choices=['1', '2', 'ignore'])
    sync_parser.add_argument('--dry-run', action='store_true')

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
    logging.getLogger('filelock').setLevel(logging.CRITICAL)

    logger = logging.getLogger(__name__)
    logger.setLevel(args.log_level)

    config = utils.get_config()

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


def get_sync_worker(entry):
    client_1, client_2 = get_clients(entry)
    return sync.SyncWorker(client_1, client_2)


def sync_command(args, config, logger):
    command = SyncCommand(args, config, logger)
    command.run()


def daemon_command(args, config, logger, terminator=lambda x: False):
    all_targets = list(config['targets'].keys())
    if not args.targets:
        targets = all_targets
    else:
        targets = args.targets

    if not targets:
        logger.info('No targets available')
        logger.info('Use "add" command first')
        return

    for target in targets:
        if target not in config['targets']:
            logger.info("Unknown target: %s", target)
            return

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

    index = 0
    while not terminator(index):
        index += 1

        to_run = defaultdict(set)
        for event in notifier.read(read_delay=args.read_delay):
            target = watch_map[event.wd]

            # Dont bother running for .index
            if event.name not in ('.index', '.s4lock'):
                to_run[target].add(event.name)

        for target, keys in to_run.items():
            entry = config['targets'][target]
            worker = get_sync_worker(entry)

            # Should ideally be setting keys to sync
            logger.info('Syncing {}'.format(worker))
            worker.sync(conflict_choice=args.conflicts)


def targets_command(args, config, logger):
    command = TargetsCommand(args, config, logger)
    command.run()


def add_command(args, config, logger):
    command = AddCommand(args, config, logger)
    command.run()


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
        entry['local_folder'] = os.path.expanduser(new_local_folder)
    if new_s3_uri:
        entry['s3_uri'] = new_s3_uri
    if new_aws_access_key_id:
        entry['aws_access_key_id'] = new_aws_access_key_id
    if new_aws_secret_access_key:
        entry['aws_secret_access_key'] = new_aws_secret_access_key
    if new_region_name:
        entry['region_name'] = new_region_name

    config['targets'][args.target] = entry

    utils.set_config(config)


def ls_command(args, config, logger):
    command = LsCommand(args, config, logger)
    command.run()


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
    utils.set_config(config)


if __name__ == '__main__':
    main(sys.argv[1:])
