#! -*- encoding: utf8 -*-
import os

from s4 import utils
from s4.commands import Command


class EditCommand(Command):
    def run(self):
        if "targets" not in self.config:
            self.logger.info("You have not added any targets yet")
            self.logger.info('Use the "add" command to do this')
            return

        if self.args.target not in self.config["targets"]:
            all_targets = sorted(list(self.config["targets"].keys()))
            self.logger.info(f'"{self.args.target}" is an unknown target')
            self.logger.info(f"Choices are: {all_targets}")
            return

        entry = self.config["targets"][self.args.target]

        local_folder = entry.get("local_folder", "")
        endpoint_url = entry.get("endpoint_url")
        s3_uri = entry.get("s3_uri", "")
        aws_access_key_id = entry.get("aws_access_key_id")
        aws_secret_access_key = entry.get("aws_secret_access_key")
        region_name = entry.get("region_name")

        new_local_folder = utils.get_input(f"local folder [{local_folder}]: ")
        new_endpoint_url = utils.get_input(f"endpoint url [{endpoint_url}]: ")
        new_s3_uri = utils.get_input(f"s3 uri [{s3_uri}]: ")
        new_aws_access_key_id = utils.get_input(
            f"AWS Access Key ID [{aws_access_key_id}]: "
        )
        secret_key_prompt = f"AWS Secret Access Key [{aws_secret_access_key}]: "
        new_aws_secret_access_key = utils.get_input(secret_key_prompt, secret=True)

        new_region_name = utils.get_input("region name [{}]: ".format(region_name))

        if new_local_folder:
            entry["local_folder"] = os.path.expanduser(new_local_folder)
        if new_s3_uri:
            entry["s3_uri"] = new_s3_uri
        if new_aws_access_key_id:
            entry["aws_access_key_id"] = new_aws_access_key_id
        if new_aws_secret_access_key:
            entry["aws_secret_access_key"] = new_aws_secret_access_key
        if new_endpoint_url:
            entry["endpoint_url"] = new_endpoint_url
        if new_region_name:
            entry["region_name"] = new_region_name

        self.config["targets"][self.args.target] = entry

        utils.set_config(self.config)
