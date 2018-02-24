#! -*- encoding: utf-8 -*-
from collections import defaultdict

# Dont crash on import if the underlying operating system does not support INotify
try:
    from inotify_simple import flags
    from s4.inotify_recursive import INotifyRecursive
    supported = True
except OSError:
    supported = False

from s4.commands import Command


class DaemonCommand(Command):
    def run(self, terminator=lambda x: False):
        if not supported:
            self.logger.info('Cannot run INotify on your operating system')
            self.logger.info('Only Linux machines are officially supported for this command')
            return

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
            worker = self.get_sync_worker(target)
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
                worker = self.get_sync_worker(target)

                # Should ideally be setting keys to sync
                self.logger.info('Syncing {}'.format(worker))
                worker.sync(conflict_choice=self.args.conflicts)
