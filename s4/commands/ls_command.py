#! -*- encoding: utf-8 -*-

from datetime import datetime

from tabulate import tabulate

from s4.clients.local import get_local_client
from s4.clients.s3 import get_s3_client


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


class LsCommand(object):
    def __init__(self, args, config, logger):
        self.args = args
        self.config = config
        self.logger = logger

    def run(self):
        if 'targets' not in self.config:
            self.logger.info('You have not added any targets yet')
            self.logger.info('Use the "add" command to do this')
            return
        if self.args.target not in self.config['targets']:
            all_targets = sorted(list(self.config['targets'].keys()))
            self.logger.info('"%s" is an unknown target', self.args.target)
            self.logger.info('Choices are: %s', all_targets)
            return

        target = self.config['targets'][self.args.target]
        client_1, client_2 = get_clients(target)

        sort_by = self.args.sort_by.lower()
        descending = self.args.descending

        keys = set(client_1.index) | set(client_2.index)

        data = []
        for key in sorted(keys):
            entry_1 = client_1.index.get(key, {})
            entry_2 = client_2.index.get(key, {})

            ts_1 = entry_1.get('local_timestamp')
            ts_2 = entry_2.get('local_timestamp')

            if self.args.show_all or ts_1 is not None:
                data.append((
                    key,
                    datetime.utcfromtimestamp(int(ts_1)) if ts_1 is not None else '<deleted>',
                    datetime.utcfromtimestamp(int(ts_2)) if ts_2 is not None else None,
                ))

        headers = ['key', 'local', 's3']
        data = sorted(data, reverse=descending, key=lambda x: x[headers.index(sort_by)])

        print(tabulate(data, headers=headers))
