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

        self.logger.info("To add a new target, please enter the following\n")

        entry = {}
        local_folder = utils.get_input(
            "local folder path to sync (leave blank for current folder): "
        )
        if not local_folder:
            local_folder = os.getcwd()

        local_folder = os.path.expanduser(local_folder)
        local_folder = os.path.abspath(local_folder)

        entry["local_folder"] = local_folder

        entry["endpoint_url"] = utils.get_input("endpoint url (leave blank for AWS): ")

        bucket = utils.get_input("S3 Bucket (required): ", required=True)
        path = utils.get_input("S3 Path (required): ", required=True)

        entry["s3_uri"] = "s3://{}/{}".format(bucket, path)

        entry["region_name"] = utils.get_input("region name (leave blank if unknown): ")

        if target is not None:
            entry["aws_access_key_id"] = self.config["targets"][target][
                "aws_access_key_id"
            ]
            entry["aws_secret_access_key"] = self.config["targets"][target][
                "aws_secret_access_key"
            ]
        else:
            entry["aws_access_key_id"] = utils.get_input(
                "AWS Access Key ID (required): ", required=True
            )
            entry["aws_secret_access_key"] = utils.get_input(
                "AWS Secret Access Key (required): ", secret=True, required=True
            )

        default_name = os.path.basename(entry["s3_uri"])
        name = utils.get_input(
            "Provide a name for this entry (leave blank to default to '{}'): ".format(
                default_name
            )
        )

        if not name:
            name = default_name

        self.logger.info(
            "\nYou can edit anything you have entered here using the 'edit' command"
        )

        self.config["targets"][name] = entry

        utils.set_config(self.config)
