# !/usr/bin/env python

from distutils.core import setup

setup(
    name='s3backup',
    packages=[],
    version='0.1.0',
    description='Autosync your folders to s3 between multiple machines',
    author='Michael Aquilina',
    license='MIT',
    author_email='michaelaquilina@gmail.com',
    url='https://github.com/MichaelAquilina/s3backup',
    keywords=['aws', 's3', 'backup'],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
