package org.pescuma.dummycassandra;

public class CassandraSuperRow
{
	private final HectorColumnFamilyFacade hector;
	private final Object rowKey;
	
	public CassandraSuperRow(HectorColumnFamilyFacade hector, Object rowKey)
	{
		this.hector = hector;
		this.rowKey = rowKey;
	}
	
	public CassandraSuperColumn getSuperColumn(Object name)
	{
		return new CassandraSuperColumn(hector, rowKey, name);
	}
	
	@SuppressWarnings("rawtypes")
	public Iterable getSuperColumnKeys()
	{
		return hector.getSuperColumnKeys(rowKey);
	}
	
	@SuppressWarnings("rawtypes")
	public Iterable getSuperColumnKeys(Object startColumnKey, Object endColumnKey)
	{
		return hector.getSuperColumnKeysSlice(rowKey, startColumnKey, endColumnKey);
	}
	
	public int getSuperColumnCount()
	{
		return hector.countSuperColumns(rowKey);
	}
}
