#! -*- encoding: utf-8 -*-

from s4 import utils
from s4.commands import Command


class RmCommand(Command):
    def run(self):
        if 'targets' not in self.config:
            self.logger.info('You have not added any targets yet')
            return
        if self.args.target not in self.config['targets']:
            all_targets = sorted(list(self.config['targets'].keys()))
            self.logger.info('"%s" is an unknown target', self.args.target)
            self.logger.info('Choices are: %s', all_targets)
            return

        del self.config['targets'][self.args.target]
        utils.set_config(self.config)
