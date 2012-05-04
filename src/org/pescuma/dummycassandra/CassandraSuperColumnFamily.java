package org.pescuma.dummycassandra;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;

public class CassandraSuperColumnFamily {

	private final HectorColumnFamilyFacade hector;

	public CassandraSuperColumnFamily(CassandraKeyspace keyspace, String name, CassandraType rowKeyType,
			CassandraType columnKeyType, CassandraType subColumnKeyType, CassandraType valueType) {
		this.hector = new HectorColumnFamilyFacade(keyspace, name, rowKeyType, columnKeyType, subColumnKeyType,
				valueType);
	}

	public String getName() {
		return hector.getName();
	}

	public CassandraType getRowKeyType() {
		return hector.getRowKeyType();
	}

	public CassandraType getColumnKeyType() {
		return hector.getColumnKeyType();
	}

	public CassandraType getSubColumnKeyType() {
		return hector.getSubColumnKeyType();
	}

	public CassandraType getValueType() {
		return hector.getValueType();
	}

	public Boolean getReplicateOnWrite() {
		return hector.getReplicateOnWrite();
	}

	public void setReplicateOnWrite(boolean replicateOnWrite) {
		hector.setReplicateOnWrite(getReplicateOnWrite());
	}

	public CassandraSuperRow getRow(Object key) {
		return new CassandraSuperRow(hector, key);
	}

	@SuppressWarnings("rawtypes")
	public Iterable getRowKeys() {
		return hector.getRowKeys();
	}

	ColumnFamilyDefinition createColumnFamilyDefinition() {
		return hector.createColumnFamilyDefinition();
	}

}
