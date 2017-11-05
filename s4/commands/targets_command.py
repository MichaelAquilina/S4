#! -*- encoding: utf8 -*-

from s4.commands import Command


class TargetsCommand(Command):
    def run(self):
        for name in sorted(self.config['targets']):
            entry = self.config['targets'][name]
            print('{}: [{} <=> {}]'.format(
                name, entry['local_folder'], entry['s3_uri']
            ))
