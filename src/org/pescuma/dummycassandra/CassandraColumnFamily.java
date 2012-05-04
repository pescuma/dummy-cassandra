package org.pescuma.dummycassandra;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;

public class CassandraColumnFamily {

	private final HectorColumnFamilyFacade hector;

	public CassandraColumnFamily(CassandraKeyspace keyspace, String name, CassandraType rowKeyType,
			CassandraType columnKeyType, CassandraType valueType) {
		this.hector = new HectorColumnFamilyFacade(keyspace, name, rowKeyType, columnKeyType, null, valueType);
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

	public CassandraType getValueType() {
		return hector.getValueType();
	}

	public Boolean getReplicateOnWrite() {
		return hector.getReplicateOnWrite();
	}

	public void setReplicateOnWrite(boolean replicateOnWrite) {
		hector.setReplicateOnWrite(getReplicateOnWrite());
	}

	public CassandraRow getRow(Object key) {
		return new CassandraRow(hector, key);
	}

	@SuppressWarnings("rawtypes")
	public Iterable getRowKeys() {
		return hector.getRowKeys();
	}

	ColumnFamilyDefinition createColumnFamilyDefinition() {
		return hector.createColumnFamilyDefinition();
	}

}
