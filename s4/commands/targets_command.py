#! -*- encoding: utf8 -*-


class TargetsCommand(object):
    def __init__(self, args, config, logger):
        self.args = args
        self.config = config
        self.logger = logger

    def run(self):
        for name in sorted(self.config['targets']):
            entry = self.config['targets'][name]
            print('{}: [{} <=> {}]'.format(
                name, entry['local_folder'], entry['s3_uri']
            ))
