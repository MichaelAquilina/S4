# -*- coding: utf-8 -*-

import gzip
import json
import zlib

import mock
import pytest

from s4 import utils


class TestTryDecompress:
    def test_bad_value(self):
        with pytest.raises(ValueError) as exc:
            utils.try_decompress(b"Not compressed data")
        assert str(exc.value) == "Unknown compression format"

    def test_gzip(self):
        body = gzip.compress(b"some data")
        assert utils.try_decompress(body) == b"some data"

    def test_zlib(self):
        body = zlib.compress(b"some data")
        assert utils.try_decompress(body) == b"some data"


@mock.patch("getpass.getpass")
@mock.patch("builtins.input")
class TestGetInput:
    def test_required(self, input_fn, getpass):
        input_fn.side_effect = ["", "", "something"]

        result = utils.get_input("give me some info", required=True)

        assert result == "something"
        assert input_fn.call_count == 3
        assert getpass.call_count == 0

    def test_not_secret(self, input_fn, getpass):
        input_fn.return_value = "foo"

        result = utils.get_input("give me some info", secret=False)

        assert result == "foo"

        assert getpass.call_count == 0
        assert input_fn.call_count == 1

    def test_blank(self, input_fn, getpass):
        input_fn.return_value = ""

        result = utils.get_input("give me some info", blank=True)

        assert result is None

        assert getpass.call_count == 0
        assert input_fn.call_count == 1

    def test_secret(self, input_fn, getpass):
        getpass.return_value = "bar"

        result = utils.get_input("give me some secret info", secret=True)

        assert result == "bar"

        assert getpass.call_count == 1
        assert input_fn.call_count == 0


class TestGetConfigFile(object):
    @mock.patch("s4.utils.CONFIG_FILE_PATH", "/i/dont/exist")
    def test_no_file(self):
        assert utils.get_config() == {"targets": {}}

    def test_correct_output(self, config_file):
        with open(config_file, "w") as fp:
            json.dump({"local_folder": "/home/someone/something"}, fp)

        assert utils.get_config() == {"local_folder": "/home/someone/something"}
