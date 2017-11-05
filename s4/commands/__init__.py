#! -*- encoding: utf8 -*-

from s4 import sync
from s4.clients.local import get_local_client
from s4.clients.s3 import get_s3_client


class Command(object):
    def __init__(self, args, config, logger):
        self.args = args
        self.config = config
        self.logger = logger

    def get_sync_worker(self, target):
        entry = self.config['targets'][target]
        client_1, client_2 = self.get_clients(entry)
        return sync.SyncWorker(client_1, client_2)

    def get_clients(self, entry):
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
