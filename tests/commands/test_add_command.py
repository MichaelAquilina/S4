# -*- encoding: utf-8 -*-

import argparse
import json
import os

import mock

from s4.commands.add_command import AddCommand

from tests import utils


@mock.patch("s4.utils.get_input")
class TestAddCommand(object):
    def test_correct_behaviour(self, get_input, config_file):
        get_input.side_effect = [
            "/home/user/Documents",
            None,
            "mybucket",
            "Documents",
            "eu-west-2",
            "aaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbb",
            "",
        ]
        args = argparse.Namespace(copy_target_credentials=None)

        command = AddCommand(args, {"targets": {}}, utils.create_logger())
        command.run()

        with open(config_file, "r") as fp:
            new_config = json.load(fp)

        expected_config = {
            "targets": {
                "Documents": {
                    "local_folder": "/home/user/Documents",
                    "endpoint_url": None,
                    "s3_uri": "s3://mybucket/Documents",
                    "aws_access_key_id": "aaaaaaaaaaaaaaaaaaaaaaaa",
                    "aws_secret_access_key": "bbbbbbbbbbbbbbbbbbbbbbbb",
                    "region_name": "eu-west-2",
                }
            }
        }
        assert new_config == expected_config

    def test_default_local_folder(self, get_input, config_file):
        get_input.side_effect = [
            None,
            None,
            "mybucket",
            "Documents",
            "eu-west-2",
            "aaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbb",
            "",
        ]
        args = argparse.Namespace(copy_target_credentials=None)

        command = AddCommand(args, {"targets": {}}, utils.create_logger())
        command.run()

        with open(config_file, "r") as fp:
            config = json.load(fp)

        assert config["targets"]["Documents"]["local_folder"] == os.getcwd()

    def test_copy_target_credentials(self, get_input, config_file):
        get_input.side_effect = [
            "/home/user/Animals",
            None,
            "mybucket",
            "Zoo",
            "us-west-2",
            "Beasts",
        ]
        args = argparse.Namespace(copy_target_credentials="bar")

        command = AddCommand(
            args,
            {
                "targets": {
                    "bar": {
                        "aws_secret_access_key": "bar-secretz",
                        "aws_access_key_id": "so-much-bar",
                    }
                }
            },
            utils.create_logger(),
        )
        command.run()

        with open(config_file, "r") as fp:
            new_config = json.load(fp)

        expected_config = {
            "targets": {
                "bar": {
                    "aws_access_key_id": "so-much-bar",
                    "aws_secret_access_key": "bar-secretz",
                },
                "Beasts": {
                    "local_folder": "/home/user/Animals",
                    "endpoint_url": None,
                    "s3_uri": "s3://mybucket/Zoo",
                    "aws_access_key_id": "so-much-bar",
                    "aws_secret_access_key": "bar-secretz",
                    "region_name": "us-west-2",
                },
            }
        }
        assert new_config == expected_config

    def test_copy_target_credentials_bad_target(self, get_input, capsys):
        get_input.side_effect = [
            "/home/user/Animals",
            "",
            "mybucket",
            "Zoo",
            "us-west-2",
            "Beasts",
        ]
        args = argparse.Namespace(copy_target_credentials="Foo")

        command = AddCommand(args, {"targets": {"bar": {}}}, utils.create_logger())
        command.run()

        out, err = capsys.readouterr()
        assert out == ""
        assert err == ('"Foo" is an unknown target\n' "Choices are: ['bar']\n")

    def test_custom_target_name(self, get_input, config_file):
        get_input.side_effect = [
            "/home/user/Music",
            None,
            "mybucket",
            "Musiccccc",
            "us-west-1",
            "1234567890",
            "abcdefghij",
            "Tunes",
        ]
        args = argparse.Namespace(copy_target_credentials=None)

        command = AddCommand(args, {"targets": {}}, utils.create_logger())
        command.run()

        with open(config_file, "r") as fp:
            new_config = json.load(fp)

        expected_config = {
            "targets": {
                "Tunes": {
                    "local_folder": "/home/user/Music",
                    "endpoint_url": None,
                    "s3_uri": "s3://mybucket/Musiccccc",
                    "aws_access_key_id": "1234567890",
                    "aws_secret_access_key": "abcdefghij",
                    "region_name": "us-west-1",
                }
            }
        }
        assert new_config == expected_config
