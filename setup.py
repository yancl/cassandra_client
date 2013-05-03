#!/usr/bin/env python
import os
import sys
from setuptools import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload -r internal')
    sys.exit()


setup(
        name = "cassandra_client",
        version = "0.10",
        description="python thrift client for cassandra",
        long_description=open("README.md").read(),
        author="yancl",
        author_email="kmoving@gmail.com",
        url='https://github.com/yancl/cassandra_client',
        classifiers=[
            'Programming Language :: Python',
        ],
        platforms='Linux',
        license='MIT License',
        zip_safe=False,
        install_requires=[
            'distribute',
            'thrift_client',
        ],
        tests_require=[
            'nose',
        ],
        packages=[  'cassandra_client', 'cassandra_client.protocol',
                    'cassandra_client.protocol.genpy',
                    'cassandra_client.protocol.genpy.cassandra']
)
