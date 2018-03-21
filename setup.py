# !/usr/bin/env python

from distutils.core import setup

try:
    from setuptools import setup, find_packages  # noqa
except ImportError:
    from distutils.core import setup

from s4 import VERSION

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.read().split('\n')

setup(
    name='s4',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    version=VERSION,
    description='Sync your folders to s3 between multiple machines',
    long_description=readme,
    install_requires=requirements,
    author='Michael Aquilina',
    license='GPLv3',
    author_email='michaelaquilina@gmail.com',
    url='https://github.com/MichaelAquilina/s4',
    python_requires=">=3.4",
    keywords='aws s3 backup sync',
    scripts=[
        'bin/s4',
        's4/cli.py',
        's4/__init__.py',
        's4/sync.py',
        's4/utils.py',
        's4/clients/__init__.py',
        's4/clients/local.py',
        's4/clients/s3.py',
    ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
