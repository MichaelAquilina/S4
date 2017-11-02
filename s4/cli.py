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


def sync_command(args, config, logger):
    command = SyncCommand(args, config, logger)
    command.run()


def daemon_command(args, config, logger, terminator=lambda x: False):
    command = DaemonCommand(args, config, logger)
    command.run(terminator)


def targets_command(args, config, logger):
    command = TargetsCommand(args, config, logger)
    command.run()


def add_command(args, config, logger):
    command = AddCommand(args, config, logger)
    command.run()


def edit_command(args, config, logger):
    command = EditCommand(args, config, logger)
    command.run()


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
