#! -*- encoding: utf-8 -*-
from collections import defaultdict

from inotify_simple import flags

from s4 import sync
from s4.clients.local import get_local_client
from s4.clients.s3 import get_s3_client
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


def get_sync_worker(entry):
    client_1, client_2 = get_clients(entry)
    return sync.SyncWorker(client_1, client_2)


class DaemonCommand(object):
    def __init__(self, args, config, logger):
        self.args = args
        self.config = config
        self.logger = logger

    def run(self, terminator=lambda x: False):
        all_targets = list(self.config['targets'].keys())
        if not self.args.targets:
            targets = all_targets
        else:
            targets = self.args.targets

        if not targets:
            self.logger.info('No targets available')
            self.logger.info('Use "add" command first')
            return

        for target in targets:
            if target not in self.config['targets']:
                self.logger.info("Unknown target: %s", target)
                return

        notifier = INotifyRecursive()
        watch_flags = flags.CREATE | flags.DELETE | flags.MODIFY

        watch_map = {}

        for target in targets:
            entry = self.config['targets'][target]
            path = entry['local_folder']
            self.logger.info("Watching %s", path)
            for wd in notifier.add_watches(path.encode('utf8'), watch_flags):
                watch_map[wd] = target

            # Check for any pending changes
            worker = get_sync_worker(entry)
            worker.sync(conflict_choice=self.args.conflicts)

        index = 0
        while not terminator(index):
            index += 1

            to_run = defaultdict(set)
            for event in notifier.read(read_delay=self.args.read_delay):
                target = watch_map[event.wd]

                # Dont bother running for .index
                if event.name not in ('.index', '.s4lock'):
                    to_run[target].add(event.name)

            for target, keys in to_run.items():
                entry = self.config['targets'][target]
                worker = get_sync_worker(entry)

                # Should ideally be setting keys to sync
                self.logger.info('Syncing {}'.format(worker))
                worker.sync(conflict_choice=self.args.conflicts)
