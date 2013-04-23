#!/usr/bin/env python

from distutils.core import setup

setup(
        name = "cassandra_client",
        version = "0.10",
        description="python thrift client for cassandra",
        maintainer="yancl",
        maintainer_email="kmoving@gmail.com",
        packages=[  'cassandra_client', 'cassandra_client.protocol',
                    'cassandra_client.protocol.genpy',
                    'cassandra_client.protocol.genpy.cassandra']
)
