import sys
sys.path.insert(0, '../')

from protocol.genpy.cassandra import Cassandra
from protocol.genpy.cassandra.ttypes import Mutation,ConsistencyLevel
from cassandra_client import CassandraMetaAPI, CassandraAPI
from thrift_client import thrift_client

class TestCassandra(object):
    def setUp(self):
        self._keyspace = 'test'
        self._cf = 'user'
        self._cassandra_meta_api = CassandraMetaAPI(handle=self._get_new_service_client(port=9160))
        self._cassandra_api = CassandraAPI(handle=self._get_new_service_client(port=9160), keyspace=self._keyspace)
        self._cassandra_meta_api.add_keyspace(name=self._keyspace, cf_names=[self._cf])

    def test_add_column_family(self):
        self._cassandra_meta_api.add_column_family(name=self._keyspace, cf_name='pin')

    def test_insert_column(self):
        self._cassandra_api.insert_column(pk='1', cf='user', name='name', value='t')

    def test_delete_column(self):
        self._cassandra_api.delete_column(pk='1', cf='user', name='name')

    def test_select_column(self):
        self._cassandra_api.select_column(pk='1', cf='user', name='name')

    def test_select_slice(self):
        self._cassandra_api.select_slice(pk='1', cf='user', start='',finish='')

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
        cbf.add(cf='user', mutations=user_mutations)

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
        cbf.add(cf='pin', mutations=pin_mutations)

        #cassandra batch
        cb = CassandraAPI.CassandraBatch()
        cb.add(pk='10001', cassandra_batch_cf=cbf)

        return self._cassandra_api.batch_update(cassandra_batch=cb)

    def _get_new_service_client(self, port):
        return thrift_client.ThriftClient(client_class=Cassandra.Client, servers=['127.0.0.1:'+str(port)])
