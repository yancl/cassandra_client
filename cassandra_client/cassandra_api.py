import time
from protocol.genpy.cassandra.ttypes import *

class CassandraMetaAPI(object):
    def __init__(self, handle,
                    key_validation_class='org.apache.cassandra.db.marshal.UTF8Type',
                    default_validation_class='org.apache.cassandra.db.marshal.UTF8Type',
                    comparator_type='org.apache.cassandra.db.marshal.UTF8Type',
                    strategy_class='org.apache.cassandra.locator.NetworkTopologyStrategy',
                    strategy_options={'datacenter1': '1'}):

        self._handle = handle
        self._key_validation_class = key_validation_class
        self._default_validation_class = default_validation_class
        self._comparator_type = comparator_type
        self._strategy_class = strategy_class
        self._strategy_options = strategy_options

    def add_keyspace(self, name, cf_names):
        cf_defs = [CfDef(keyspace=name,
                    name=cf_name,
                    key_validation_class=self._key_validation_class,
                    default_validation_class=self._default_validation_class,
                    comparator_type=self._comparator_type)
                    for cf_name in cf_names]

        ks_def = KsDef(name=name,
                strategy_class=self._strategy_class,
                strategy_options=self._strategy_options, 
                cf_defs=cf_defs)
        return self._handle.system_add_keyspace(ks_def=ks_def)

    def drop_keyspace(self, name):
        self._handle.system_drop_keyspace(name)

    def add_column_family(self, name, cf_name):
        self._set_keyspace(name)
        cf_def = CfDef(keyspace=name,
                    name=cf_name,
                    key_validation_class=self._key_validation_class,
                    default_validation_class=self._default_validation_class,
                    comparator_type=self._comparator_type)

        return self._handle.system_add_column_family(cf_def=cf_def)

    def drop_column_family(self, name, cf_name):
        self._set_keyspace(name)
        self._handle.system_drop_column_family(cf_name)

    def describe_keyspace(self, keyspace):
        return self._handle.describe_keyspace(keyspace)

    def _set_keyspace(self, keyspace):
        self._handle.set_keyspace(keyspace)

class CassandraAPI(object):
    class CassandraBatchCF(object):
        def __init__(self):
            self._m = {}

        def add(self, cf, mutations):
            self._m[cf] = mutations

        def get(self):
            return self._m

    class CassandraBatch(object):
        def __init__(self):
            self._m = {}

        def add(self, pk, cassandra_batch_cf):
            self._m[pk] = cassandra_batch_cf

        def get(self):
            return self._m

    def __init__(self, handle, keyspace,
            read_cons_level=ConsistencyLevel.ONE,
            write_cons_level=ConsistencyLevel.ONE):

        self._handle = handle
        self._keyspace = keyspace
        self._set_keyspace(self._keyspace)
        self._read_cons_level = read_cons_level
        self._write_cons_level = write_cons_level
        self._handle.add_callback('post_connect', self._post_connect_callback)

    def insert_column(self, pk, cf, name, value):
        column_parent = ColumnParent(column_family=cf)
        column = Column(name=name, value=value, timestamp=time.time())
        self._handle.insert(key=pk,
                    column_parent=column_parent,
                    column=column,
                    consistency_level=self._write_cons_level)

    def delete_column(self, pk, cf, name):
        column_path = ColumnPath(column_family=cf, column=name)
        self._handle.remove(pk,
              column_path = column_path,
              timestamp=time.time(),
              consistency_level=self._write_cons_level)

    def select_column(self, pk, cf, name):
        column_parent = ColumnParent(column_family=cf)
        slice_range = SliceRange(start=name, finish=name)
        predicate = SlicePredicate(slice_range=slice_range)
        return self._handle.get_slice(key=pk,
                                  column_parent=column_parent,
                                  predicate=predicate,
                                  consistency_level=self._read_cons_level)

    def select_slice(self, pk, cf, start="", finish="",reversed=0,count=100):
        column_parent = ColumnParent(column_family=cf)
        slice_range = SliceRange(start=start, finish=finish, reversed=reversed, count=count)
        predicate = SlicePredicate(slice_range=slice_range)
        return self._handle.get_slice(key=pk,
                                  column_parent=column_parent,
                                  predicate=predicate,
                                  consistency_level=self._read_cons_level)

    def get_range(self, cf, columns, start_key="", end_key="", count=100):
        column_parent = ColumnParent(column_family=cf)
        #slice_range = SliceRange(start="", finish="", reversed=0, count=100)
        #predicate = SlicePredicate(slice_range=slice_range)
        predicate = SlicePredicate(column_names=columns)
        range=KeyRange(start_key=start_key, end_key=end_key, count=count)
        return self._handle.get_range_slices(column_parent=column_parent,
                                                predicate=predicate,
                                                range=range,
                                                consistency_level=self._read_cons_level)

    def batch_update(self, cassandra_batch):
        mutation_map = {}
        batches = cassandra_batch.get()
        for (pk, batch_cf) in batches.iteritems():
            cf_map = {}
            batch_cfs = batch_cf.get()
            for (cf, mutations) in batch_cfs.iteritems():
                cf_map[cf] = mutations
            mutation_map[pk] = cf_map
        self._handle.batch_mutate(mutation_map=mutation_map,
                    consistency_level=self._write_cons_level)

    def add_counter(self, pk, cf, name, value):
        column_parent = ColumnParent(column_family=cf)
        column = CounterColumn(name=name, value=value)
        self._handle.add(key=pk,
                        column_parent=column_parent,
                        column=column,
                        consistency_level=self._write_cons_level)

    def _set_keyspace(self, keyspace):
        self._handle.set_keyspace(keyspace)

    def _post_connect_callback(self):
        self._set_keyspace(self._keyspace)
