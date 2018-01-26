# -*- coding: utf-8 -*-
import boto3

from s4 import utils
from s4.commands import Command


class AutoSetupCommand(Command):
    def run(self):
        target = self.args.copy_target_credentials
        all_targets = list(self.config['targets'].keys())
        if target is not None and target not in all_targets:
            self.logger.info('"%s" is an unknown target', target)
            self.logger.info('Choices are: %s', all_targets)
            return

        entry = {}
        if target is not None:
            entry['aws_access_key_id'] = self.config['targets'][target]['aws_access_key_id']
            entry['aws_secret_access_key'] = self.config['targets'][target]['aws_secret_access_key']
        else:
            entry['aws_access_key_id'] = utils.get_input('AWS Access Key ID: ')
            entry['aws_secret_access_key'] = utils.get_input('AWS Secret Access Key: ', secret=True)

        client = boto3.client(
            's3',
            aws_secret_access_key=entry['aws_secret_access_key'],
            aws_access_key_id=entry['aws_access_key_id'],
            region_name='eu-west-2',
        )

        resp = client.list_buckets()
        print([b['Name'] for b in resp['Buckets']])
