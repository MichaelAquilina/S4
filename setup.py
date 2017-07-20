# !/usr/bin/env python

from distutils.core import setup

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.md') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.read().split('\n')

setup(
    name='s4',
    packages=[],
    version='0.1.5',
    description='Sync your folders to s3 between multiple machines',
    long_description=readme,
    install_requires=requirements,
    author='Michael Aquilina',
    license='MIT',
    author_email='michaelaquilina@gmail.com',
    url='https://github.com/MichaelAquilina/s3backup',
    keywords=['aws', 's3', 'backup', 'sync'],
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
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
