# -*- coding: utf-8 -*-

import argparse
import logging
import sys

from s4 import VERSION
from s4 import utils
from s4.commands.add_command import AddCommand
from s4.commands.daemon_command import DaemonCommand
from s4.commands.edit_command import EditCommand
from s4.commands.ls_command import LsCommand
from s4.commands.rm_command import RmCommand
from s4.commands.sync_command import SyncCommand
from s4.commands.targets_command import TargetsCommand


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

    daemon_parser = subparsers.add_parser('daemon', help='Run S4 sync continiously', aliases=['d'])
    daemon_parser.add_argument('targets', nargs='*')
    daemon_parser.add_argument('--read-delay', default=1000, type=int)
    daemon_parser.add_argument('--conflicts', default='ignore', choices=['1', '2', 'ignore'])

    add_parser = subparsers.add_parser('add', help='Add a new Target to synchronise', aliases=['a'])
    add_parser.add_argument(
        '--copy-target-credentials',
        '-C',
        help="Copy credentials from an existing target instead of typing them in again"
    )

    sync_parser = subparsers.add_parser('sync', help='Synchronise Targets with S3', aliases=['s'])
    sync_parser.add_argument('targets', nargs='*')
    sync_parser.add_argument('--conflicts', default=None, choices=['1', '2', 'ignore'])
    sync_parser.add_argument('--dry-run', action='store_true')

    edit_parser = subparsers.add_parser('edit', help='Edit Target details', aliases=['e'])
    edit_parser.add_argument('target')

    subparsers.add_parser('targets', help='Print available Targets', aliases=['t'])

    subparsers.add_parser('version', help='Print S4 Version', aliases=['v'])

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
        command = None
        if args.command in ('version', 'v'):
            print(VERSION)
            return
        elif args.command in ('sync', 's'):
            command = SyncCommand(args, config, logger)
        elif args.command in ('targets', 't'):
            command = TargetsCommand(args, config, logger)
        elif args.command in ('add', 'a'):
            command = AddCommand(args, config, logger)
        elif args.command in ('edit', 'e'):
            command = EditCommand(args, config, logger)
        elif args.command == 'ls':
            command = LsCommand(args, config, logger)
        elif args.command == 'rm':
            command = RmCommand(args, config, logger)
        elif args.command in ('daemon', 'd'):
            command = DaemonCommand(args, config, logger)

        if command:
            try:
                command.run()
            except Exception as e:
                logger.error('An unhandled error has occurred: %s', e)
                # Only display a scary stack trace to the user if in DEBUG mode
                if args.log_level == 'DEBUG':
                    raise e
        else:
            parser.print_help()
    except KeyboardInterrupt:
        pass


def entry_point():
    main(sys.argv[1:])
