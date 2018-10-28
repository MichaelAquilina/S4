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
            "local folder (leave blank for current folder): "
        )
        if not local_folder:
            local_folder = os.getcwd()

        entry["local_folder"] = os.path.expanduser(local_folder)

        entry["endpoint_url"] = utils.get_input("endpoint url (leave blank for AWS): ")
        entry["s3_uri"] = utils.get_input("s3 uri: ")
        entry["region_name"] = utils.get_input("region name: ")

        if target is not None:
            entry["aws_access_key_id"] = self.config["targets"][target][
                "aws_access_key_id"
            ]
            entry["aws_secret_access_key"] = self.config["targets"][target][
                "aws_secret_access_key"
            ]
        else:
            entry["aws_access_key_id"] = utils.get_input("AWS Access Key ID: ")
            entry["aws_secret_access_key"] = utils.get_input(
                "AWS Secret Access Key: ", secret=True
            )

        default_name = os.path.basename(entry["s3_uri"])
        name = utils.get_input(
            "Provide a name for this entry [{}]: ".format(default_name)
        )

        if not name:
            name = default_name

        self.config["targets"][name] = entry

        utils.set_config(self.config)
