Changelog
=========

0.4.4 - 19 May 2019
-------------------
* Fix README.rst bug which prevented package upload

0.4.3 - 19 May 2019
-------------------
* Add fallback mechanism for decompressing data with no known header

0.4.2 - 06 May 2019
-------------------
* Upgrade boto3 minimum requirement to fix a vulnerability in a urllib3 dependency

0.4.1 - 15-November-2018
------------------------
* Fix bug where leaving region blank would result in broken target

0.4.0 - 30-October-2018
-----------------------

* Major improvements to the add command
* Add "Running from Source" section to README


0.3.0 - 28-October-2018
-----------------------

* Support alternative services like Minio (Resolves #134)


0.2.21 - 28-October-2018
------------------------

* Fix crash on windows due to not supporting inotify


0.2.20 - 26-July-2018
---------------------

* Add missing README entry to pypi

0.2.19 - 25-July-2018
---------------------

* Drop support for python 3.4
* Remove enum34 and scandir as requirements
* use poetry package manager
* Migrate to CircleCI 2.0

0.2.18 - 1-July-2018
--------------------

* Display Total Size in ls outputs
* skip 0.2.17 due to taging issue

0.2.16 - 18-June-2018
---------------------

* Use pathspec package for determining patterns for ignoring files

0.2.15 - 20-May-2018
--------------------
* Use more reliable console_scripts to generate executable shim

0.2.14 - 08-May-2018
--------------------
* Fix command short alias for version


0.2.13 - 08-May-2018
--------------------
* Add short aliases for most commands

0.2.12
------
* Fix for older versions of ubuntu which dont support zlib

0.2.11
------
* Fix RestructuredText bug on PyPi

0.2.10 - 03-April-2018
----------------------

* Add sections to the beginning of the README
* Fix "Other Subcommands" spelling error in README

0.2.9 - 21-March-2018
---------------------

* Upgrade dev-requirements
* Fix #106 (Unable to handle multi word targets)
* Add cache directories to .gitignore
* Drop support for python 3.3
* Start using CHANGELOGS
