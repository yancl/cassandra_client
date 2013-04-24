import sys
sys.path.insert(0, '../cassandra_client')

import time
from protocol.genpy.cassandra import Cassandra
from protocol.genpy.cassandra.ttypes import *
from cassandra_client import CassandraMetaAPI, CassandraAPI
from thrift_client import thrift_client

from nose.tools import raises, ok_, eq_

def get_new_service_client(port):
    return thrift_client.ThriftClient(client_class=Cassandra.Client,
                        servers=['127.0.0.1:'+str(port)])
                        #servers=['192.168.0.186:'+str(port),'192.168.0.187:'+str(port)])


keyspace = 'test'
cf = 'cf'

def setUp():
    print 'add test keyspace'
    cassandra_meta_api = CassandraMetaAPI(handle=get_new_service_client(port=9160))
    cassandra_meta_api.add_keyspace(name=keyspace, cf_names=['cf'])

def tearDown():
    print 'drop test keyspace'
    cassandra_meta_api = CassandraMetaAPI(handle=get_new_service_client(port=9160))
    cassandra_meta_api.drop_keyspace(name=keyspace)

class TestCassandra(object):
    def setUp(self):
        self._cassandra_meta_api = CassandraMetaAPI(handle=get_new_service_client(port=9160))
        self._cassandra_api = CassandraAPI(handle=get_new_service_client(port=9160), keyspace=keyspace)

    def tearDown(self):
        pass

    def test_add_column_family(self):
        self._cassandra_meta_api.add_column_family(name=keyspace, cf_name='cf0')
        self._cassandra_meta_api.drop_column_family(name=keyspace, cf_name='cf0')

    def test_select_column(self):
        self._cassandra_api.insert_column(pk='1', cf=cf, name='name', value='v')
        v = self._cassandra_api.select_column(pk='1', cf=cf, name='name')
        eq_(v[0].column.value, 'v')

    def test_select_slice(self):
        self._cassandra_api.insert_column(pk='2', cf=cf, name='name0', value='v0')
        self._cassandra_api.insert_column(pk='2', cf=cf, name='name1', value='v1')
        vs = self._cassandra_api.select_slice(pk='2', cf=cf, start='',finish='')
        eq_(set([vs[0].column.value, vs[1].column.value]), set(['v0', 'v1']))

    def test_get_range(self):
        vs = self._cassandra_api.get_range(cf=cf, columns=[], start_key='', end_key='')

    def test_batch_update(self):
        timestamp = time.time()
        #cassandra batch column family
        cbf = CassandraAPI.CassandraBatchCF()
        user_mutations = []
        user_mutations.append(Mutation(column_or_supercolumn=
                ColumnOrSuperColumn(
                    column=Column(
                            name='age',
                            value='20',
                            timestamp=timestamp))))
        user_mutations.append(Mutation(column_or_supercolumn=
                ColumnOrSuperColumn(
                    column=Column(
                            name='location',
                            value='ShangHai',
                            timestamp=timestamp))))
        cbf.add(cf=cf, mutations=user_mutations)

        pin_mutations = []
        pin_mutations.append(Mutation(column_or_supercolumn=
                ColumnOrSuperColumn(
                    column=Column(
                            name='url',
                            value='www.kantuban.com',
                            timestamp=timestamp))))
        pin_mutations.append(Mutation(column_or_supercolumn=
                ColumnOrSuperColumn(
                    column=Column(
                            name='name',
                            value='love.jpg',
                            timestamp=timestamp))))
        cbf.add(cf=cf, mutations=pin_mutations)

        #cassandra batch
        cb = CassandraAPI.CassandraBatch()
        cb.add(pk='10001', cassandra_batch_cf=cbf)

        self._cassandra_api.batch_update(cassandra_batch=cb)
