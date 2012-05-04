package org.pescuma.dummycassandra;

import java.util.Map;

@SuppressWarnings("unchecked")
public class CassandraSuperColumn {

	private final HectorColumnFamilyFacade hector;
	private final Object rowKey;
	private final Object superColumnKey;

	public CassandraSuperColumn(HectorColumnFamilyFacade columnFamily, Object rowKey, Object superColumnKey) {
		this.hector = columnFamily;
		this.rowKey = rowKey;
		this.superColumnKey = superColumnKey;
	}

	public void incrementCounter(Object column, long toAdd) {
		if (hector.getValueType() != CassandraType.Counter)
			throw new IllegalStateException();

		hector.mutator().insertCounter(rowKey, hector.getName(),
				hector.createSuperCounterColumn(superColumnKey, column, toAdd));
	}

	public void insert(Object column, Object value) {
		if (hector.getValueType() == CassandraType.Counter)
			throw new IllegalStateException();

		hector.mutator().insert(rowKey, hector.getName(), hector.createSuperColumn(superColumnKey, column, value));
	}

	public void delete(Object column) {
		if (hector.getValueType() == CassandraType.Counter)
			throw new IllegalStateException();

		hector.mutator().delete(rowKey, hector.getName(), column, hector.getColumnSerializer());
	}

	@SuppressWarnings("rawtypes")
	public Map getColumns() {
		return hector.getSubColumns(rowKey, superColumnKey);
	}

	@SuppressWarnings("rawtypes")
	public Iterable getColumnNames() {
		return hector.getSubColumnKeys(rowKey, superColumnKey);
	}

	public Object getColumn(Object columnKey) {
		return hector.getValue(rowKey, superColumnKey, columnKey);
	}

	public int getColumnCount() {
		return hector.countSubColumns(rowKey, superColumnKey);
	}

}
