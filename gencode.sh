#!/bin/bash
cd protocol && thrift --gen py -out genpy cassandra.thrift
