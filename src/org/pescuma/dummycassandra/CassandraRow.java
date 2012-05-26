package org.pescuma.dummycassandra;

import java.util.Map;

@SuppressWarnings("unchecked")
public class CassandraRow
{
	private final HectorColumnFamilyFacade hector;
	private final Object rowKey;
	
	public CassandraRow(HectorColumnFamilyFacade hector, Object rowKey)
	{
		this.hector = hector;
		this.rowKey = rowKey;
	}
	
	public void incrementCounter(Object column, long toAdd)
	{
		if (hector.getValueType() != CassandraType.Counter)
			throw new IllegalStateException("You can only call incrementCounter on a Counter column");
		
		hector.mutator().insertCounter(rowKey, hector.getName(), hector.createCounterColumn(column, toAdd));
	}
	
	public void insertColumn(Object column, Object value)
	{
		if (hector.getValueType() == CassandraType.Counter)
			throw new IllegalStateException("You can't call incrementCounter on a Counter column");
		
		hector.mutator().insert(rowKey, hector.getName(), hector.createColumn(column, value));
	}
	
	public void deleteColumn(Object column)
	{
		if (hector.getValueType() == CassandraType.Counter)
			throw new IllegalStateException("You can't delete a Counter column");
		
		hector.mutator().delete(rowKey, hector.getName(), column, hector.getColumnSerializer());
	}
	
	@SuppressWarnings("rawtypes")
	public Map getColumns()
	{
		return hector.getColumns(rowKey);
	}
	
	@SuppressWarnings("rawtypes")
	public Map getColumns(Object startColumnKey, Object endColumnKey)
	{
		return hector.getColumnsSlice(rowKey, startColumnKey, endColumnKey);
	}
	
	@SuppressWarnings("rawtypes")
	public Iterable getColumnNames()
	{
		return hector.getColumnKeys(rowKey);
	}
	
	@SuppressWarnings("rawtypes")
	public Iterable getColumnNames(Object startColumnKey, Object endColumnKey)
	{
		return hector.getColumnKeysSlice(rowKey, startColumnKey, endColumnKey);
	}
	
	public Object getColumn(Object columnKey)
	{
		return hector.getValue(rowKey, columnKey);
	}
	
	public int getColumnCount()
	{
		return hector.countColumns(rowKey);
	}
}
