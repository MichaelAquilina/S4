#! -*- encoding: utf8 -*
import os

from s4 import utils
from s4.commands import Command


class AddCommand(Command):
    def run(self):
        target = self.args.copy_target_credentials

        all_targets = list(self.config["targets"].keys())
        if target is not None and target not in all_targets:
            self.logger.info('"%s" is an unknown target', target)
            self.logger.info("Choices are: %s", all_targets)
            return

        if target is not None:
            target_config = self.config["targets"][target]

            aws_access_key_id = target_config["aws_access_key_id"]
            aws_secret_access_key = target_config["aws_secret_access_key"]
        else:
            aws_access_key_id = None
            aws_secret_access_key = None

        self.logger.info("To add a new target, please enter the following\n")

        local_folder = utils.get_input(
            "local folder path to sync [leave blank for current folder]: "
        )
        endpoint_url = utils.get_input(
            "endpoint url [leave blank for AWS]: ", blank=True
        )
        bucket = utils.get_input("S3 Bucket [required]: ", required=True)
        path = utils.get_input("S3 Path: ")
        region_name = utils.get_input(
            "region name [leave blank if unknown]: ", blank=True
        )

        if aws_access_key_id is None:
            aws_access_key_id = utils.get_input(
                "AWS Access Key ID [required]: ", required=True
            )
            aws_secret_access_key = utils.get_input(
                "AWS Secret Access Key [required]: ", secret=True, required=True
            )

        default_name = os.path.basename(path)
        name = utils.get_input(
            "Provide a name for this entry [leave blank to default to '{}']: ".format(
                default_name
            )
        )
        name = name or default_name
        if not local_folder:
            local_folder = os.getcwd()

        local_folder = os.path.expanduser(local_folder)
        local_folder = os.path.abspath(local_folder)

        self.config["targets"][name] = {
            "local_folder": local_folder,
            "endpoint_url": endpoint_url,
            "s3_uri": "s3://{}/{}".format(bucket, path),
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }
        utils.set_config(self.config)

        self.logger.info(
            "\nTarget has been added. Start syncing with the 'sync' command"
        )
        self.logger.info(
            "You can edit anything you have entered here using the 'edit' command"
        )
