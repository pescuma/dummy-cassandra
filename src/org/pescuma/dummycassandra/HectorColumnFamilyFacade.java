package org.pescuma.dummycassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.CounterSuperRow;
import me.prettyprint.hector.api.beans.CounterSuperRows;
import me.prettyprint.hector.api.beans.CounterSuperSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.HCounterSuperColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.MultigetSuperSliceCounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceCounterQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SubCountQuery;
import me.prettyprint.hector.api.query.SubSliceCounterQuery;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.query.SuperCountQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.joda.time.DateTime;

import com.eaio.uuid.UUID;

@SuppressWarnings({ "unchecked", "rawtypes" })
class HectorColumnFamilyFacade
{
	private final CassandraKeyspace keyspace;
	private final String name;
	private final CassandraType rowKeyType;
	private final CassandraType columnKeyType;
	private final CassandraType subColumnKeyType;
	private final CassandraType valueType;
	private Boolean replicateOnWrite;
	
	HectorColumnFamilyFacade(CassandraKeyspace keyspace, String name, CassandraType rowKeyType,
			CassandraType columnKeyType, CassandraType subColumnKeyType, CassandraType valueType)
	{
		if (rowKeyType == CassandraType.Counter || columnKeyType == CassandraType.Counter)
			throw new IllegalArgumentException("Only value can be of type Counter");
		
		this.keyspace = keyspace;
		this.name = name;
		this.rowKeyType = rowKeyType;
		this.columnKeyType = columnKeyType;
		this.subColumnKeyType = subColumnKeyType;
		this.valueType = valueType;
	}
	
	String getName()
	{
		return name;
	}
	
	CassandraType getRowKeyType()
	{
		return rowKeyType;
	}
	
	CassandraType getColumnKeyType()
	{
		return columnKeyType;
	}
	
	CassandraType getSubColumnKeyType()
	{
		return subColumnKeyType;
	}
	
	CassandraType getValueType()
	{
		return valueType;
	}
	
	void setReplicateOnWrite(Boolean replicateOnWrite)
	{
		this.replicateOnWrite = replicateOnWrite;
	}
	
	Boolean getReplicateOnWrite()
	{
		// Counters must have replicateOnWrite == true
		// http://wiki.apache.org/cassandra/Counters
		if (replicateOnWrite == null && valueType == CassandraType.Counter)
			return Boolean.TRUE;
		
		return replicateOnWrite;
	}
	
	ColumnFamilyDefinition createColumnFamilyDefinition()
	{
		ColumnFamilyDefinition def = HFactory.createColumnFamilyDefinition(keyspace.getName(), name);
		
		String keyValidationClass = getKeyValidationClass();
		if (keyValidationClass != null)
			def.setKeyValidationClass(keyValidationClass);
		
		ComparatorType comparatorType = getComparatorType();
		if (comparatorType != null)
			def.setComparatorType(comparatorType);
		
		String valueClass = getValueClass();
		if (valueClass != null)
			def.setDefaultValidationClass(valueClass);
		
		Boolean ror = getReplicateOnWrite();
		if (ror != null)
			def.setReplicateOnWrite(ror);
		
		if (subColumnKeyType != null)
		{
			def.setColumnType(ColumnType.SUPER);
			
			ComparatorType subComparatorType = getSubComparatorType();
			if (subComparatorType != null)
				def.setSubComparatorType(subComparatorType);
		}
		
		return def;
	}
	
	String getKeyValidationClass()
	{
		return getValidationClass(rowKeyType);
	}
	
	ComparatorType getComparatorType()
	{
		return getComparatorType(columnKeyType);
	}
	
	ComparatorType getSubComparatorType()
	{
		return getComparatorType(subColumnKeyType);
	}
	
	String getValueClass()
	{
		return getValidationClass(valueType);
	}
	
	AbstractSerializer getKeySerializer()
	{
		return getSerializer(rowKeyType);
	}
	
	AbstractSerializer getColumnSerializer()
	{
		return getSerializer(columnKeyType);
	}
	
	AbstractSerializer getSubColumnSerializer()
	{
		return getSerializer(subColumnKeyType);
	}
	
	AbstractSerializer getValueSerializer()
	{
		return getSerializer(valueType);
	}
	
	// Mutators ///////////////////////////////////////////////////////////////
	
	Mutator mutator()
	{
		return HFactory.createMutator(keyspace.keyspace, getKeySerializer());
	}
	
	HColumn createColumn(Object column, Object value)
	{
		column = toCassandra(column, columnKeyType);
		value = toCassandra(value, valueType);
		
		return HFactory.createColumn(column, value, getColumnSerializer(), getValueSerializer());
	}
	
	HCounterColumn createCounterColumn(Object column, long value)
	{
		column = toCassandra(column, columnKeyType);
		
		return HFactory.createCounterColumn(column, value, getColumnSerializer());
	}
	
	HSuperColumn createSuperColumn(Object superColumn, Object column, Object value)
	{
		superColumn = toCassandra(superColumn, columnKeyType);
		column = toCassandra(column, subColumnKeyType);
		value = toCassandra(value, valueType);
		
		List cols = new ArrayList();
		cols.add(HFactory.createColumn(column, value, getSubColumnSerializer(), getValueSerializer()));
		return HFactory.createSuperColumn(superColumn, cols, getColumnSerializer(), getSubColumnSerializer(),
				getValueSerializer());
	}
	
	HCounterSuperColumn createSuperCounterColumn(Object superColumn, Object column, long value)
	{
		superColumn = toCassandra(superColumn, columnKeyType);
		column = toCassandra(column, subColumnKeyType);
		
		List cols = new ArrayList();
		cols.add(HFactory.createCounterColumn(column, value, getSubColumnSerializer()));
		return HFactory.createCounterSuperColumn(superColumn, cols, getColumnSerializer(), getSubColumnSerializer());
	}
	
	private static Object toCassandra(Object obj, CassandraType type)
	{
		if (obj == null || type == null)
			return obj;
		
		switch (type)
		{
			case TimeUUID:
				// TODO This is not good
				if (obj instanceof Date)
					return uuidForDate((Date) obj);
				if (obj instanceof DateTime)
					return uuidForDate(((DateTime) obj).toDate());
				throw new IllegalArgumentException("Invalid object type for TimeUUID column: " + obj.getClass());
				
			default:
				return obj;
		}
	}
	
	// From http://wiki.apache.org/cassandra/FAQ#working_with_timeuuid_in_java
	private static UUID uuidForDate(Date d)
	{
		/*
		 * Magic number obtained from #cassandra's thobbs, who claims to have
		 * stolen it from a Python library.
		 */
		final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
		
		long origTime = d.getTime();
		long time = origTime * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
		long timeLow = time & 0xffffffffL;
		long timeMid = time & 0xffff00000000L;
		long timeHi = time & 0xfff000000000000L;
		long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48);
		return new UUID(upperLong, 0xC000000000000000L);
	}
	
	// Queries ////////////////////////////////////////////////////////////////
	
	Iterable getRowKeys()
	{
		if (subColumnKeyType != null)
		{
			if (valueType == CassandraType.Counter)
				return new KeyIteratorForSuperColumnCounters(keyspace.keyspace, name, getKeySerializer());
			else
				return new KeyIteratorForSuperColumn(keyspace.keyspace, name, getKeySerializer());
		}
		else
		{
			if (valueType == CassandraType.Counter)
				return new KeyIteratorForCounters(keyspace.keyspace, name, getKeySerializer());
			else
				return new KeyIterator(keyspace.keyspace, name, getKeySerializer());
		}
	}
	
	Iterable getColumnKeys(Object rowKey)
	{
		return getColumnKeysSlice(rowKey, null, null);
	}
	
	Iterable getColumnKeysSlice(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		List result = new LinkedList();
		if (valueType == CassandraType.Counter)
		{
			for (HCounterColumn col : queryCounterColumns(rowKey, startColumnKey, endColumnKey))
				result.add(col.getName());
		}
		else
		{
			for (HColumn col : queryColumns(rowKey, startColumnKey, endColumnKey))
				result.add(col.getName());
		}
		return result;
	}
	
	int countColumns(Object rowKey)
	{
		CountQuery query = HFactory.createCountQuery(keyspace.keyspace, getKeySerializer(), getColumnSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setRange(null, null, Integer.MAX_VALUE);
		return (Integer) query.execute().get();
	}
	
	Map getColumns(Object rowKey)
	{
		return getColumnsSlice(rowKey, null, null);
	}
	
	Map getColumnsSlice(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		Map result = new HashMap();
		if (valueType == CassandraType.Counter)
		{
			for (HCounterColumn col : queryCounterColumns(rowKey, startColumnKey, endColumnKey))
				result.put(col.getName(), col.getValue());
		}
		else
		{
			for (HColumn col : queryColumns(rowKey, startColumnKey, endColumnKey))
				result.put(col.getName(), col.getValue());
		}
		return result;
	}
	
	private List<HColumn> queryColumns(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		SliceQuery query = HFactory.createSliceQuery(keyspace.keyspace, getKeySerializer(), getColumnSerializer(),
				getValueSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setRange(startColumnKey, endColumnKey, false, Integer.MAX_VALUE);
		
		QueryResult<ColumnSlice> queryResult = query.execute();
		if (queryResult == null)
			return null;
		
		ColumnSlice slice = queryResult.get();
		if (slice == null)
			return null;
		
		List<HColumn> columns = slice.getColumns();
		return columns;
	}
	
	private List<HCounterColumn> queryCounterColumns(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		SliceCounterQuery query = HFactory.createCounterSliceQuery(keyspace.keyspace, getKeySerializer(),
				getColumnSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setRange(startColumnKey, endColumnKey, false, Integer.MAX_VALUE);
		
		QueryResult<CounterSlice> queryResult = query.execute();
		if (queryResult == null)
			return null;
		
		CounterSlice slice = queryResult.get();
		if (slice == null)
			return null;
		
		List<HCounterColumn> columns = slice.getColumns();
		return columns;
	}
	
	Object getValue(Object rowKey, Object columnKey)
	{
		if (valueType == CassandraType.Counter)
		{
			CounterQuery query = HFactory.createCounterColumnQuery(keyspace.keyspace, getKeySerializer(),
					getColumnSerializer());
			query.setColumnFamily(name);
			query.setKey(rowKey);
			query.setName(columnKey);
			
			QueryResult<HCounterColumn> result = query.execute();
			if (result == null)
				return null;
			
			HCounterColumn hColumn = result.get();
			if (hColumn == null)
				return null;
			
			return hColumn.getValue();
		}
		else
		{
			ColumnQuery query = HFactory.createColumnQuery(keyspace.keyspace, getKeySerializer(),
					getColumnSerializer(), getValueSerializer());
			query.setColumnFamily(name);
			query.setKey(rowKey);
			query.setName(columnKey);
			
			QueryResult<HColumn> result = query.execute();
			if (result == null)
				return null;
			
			HColumn hColumn = result.get();
			if (hColumn == null)
				return null;
			
			return hColumn.getValue();
		}
	}
	
	Iterable getSuperColumnKeys(Object rowKey)
	{
		return getSuperColumnKeysSlice(rowKey, null, null);
	}
	
	Iterable getSuperColumnKeysSlice(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		List result = new LinkedList();
		if (valueType == CassandraType.Counter)
		{
			for (HCounterSuperColumn col : querySuperCounterColumns(rowKey, startColumnKey, endColumnKey))
				result.add(col.getName());
		}
		else
		{
			for (HSuperColumn col : querySuperColumns(rowKey, startColumnKey, endColumnKey))
				result.add(col.getName());
		}
		return result;
	}
	
	Iterable getSubColumnKeys(Object rowKey, Object superColumnKey)
	{
		return getSubColumnKeysSlice(rowKey, superColumnKey, null, null);
	}
	
	Iterable getSubColumnKeysSlice(Object rowKey, Object superColumnKey, Object startColumnKey, Object endColumnKey)
	{
		List result = new LinkedList();
		if (valueType == CassandraType.Counter)
		{
			for (HCounterColumn col : querySubCounterColumns(rowKey, superColumnKey, null, startColumnKey, endColumnKey))
				result.add(col.getName());
		}
		else
		{
			for (HColumn col : querySubColumns(rowKey, superColumnKey, null, startColumnKey, endColumnKey))
				result.add(col.getName());
		}
		return result;
	}
	
	Map getSubColumns(Object rowKey, Object superColumnKey)
	{
		return getSubColumnsSlice(rowKey, superColumnKey, null, null);
	}
	
	Map getSubColumnsSlice(Object rowKey, Object superColumnKey, Object startColumnKey, Object endColumnKey)
	{
		Map result = new HashMap();
		if (valueType == CassandraType.Counter)
		{
			for (HCounterColumn col : querySubCounterColumns(rowKey, superColumnKey, null, startColumnKey, endColumnKey))
				result.put(col.getName(), col.getValue());
		}
		else
		{
			for (HColumn col : querySubColumns(rowKey, superColumnKey, null, startColumnKey, endColumnKey))
				result.put(col.getName(), col.getValue());
		}
		return result;
	}
	
	int countSuperColumns(Object rowKey)
	{
		SuperCountQuery query = HFactory.createSuperCountQuery(keyspace.keyspace, getKeySerializer(),
				getColumnSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setRange(null, null, Integer.MAX_VALUE);
		return (Integer) query.execute().get();
	}
	
	int countSubColumns(Object rowKey, Object superColumnKey)
	{
		SubCountQuery query = HFactory.createSubCountQuery(keyspace.keyspace, getKeySerializer(),
				getColumnSerializer(), getSubColumnSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setSuperColumn(superColumnKey);
		query.setRange(null, null, Integer.MAX_VALUE);
		return (Integer) query.execute().get();
	}
	
	Object getValue(Object rowKey, Object superColumnKey, Object columnKey)
	{
		if (valueType == CassandraType.Counter)
		{
			List<HCounterColumn> columns = querySubCounterColumns(rowKey, superColumnKey, columnKey, null, null);
			if (columns.size() < 1)
				return null;
			
			HCounterColumn hSubColumn = columns.get(0);
			if (hSubColumn == null)
				return null;
			
			return hSubColumn.getValue();
		}
		else
		{
			List<HColumn> columns = querySubColumns(rowKey, superColumnKey, columnKey, null, null);
			if (columns.size() < 1)
				return null;
			
			HColumn hSubColumn = columns.get(0);
			if (hSubColumn == null)
				return null;
			
			return hSubColumn.getValue();
		}
	}
	
	/**
	 * Pass columnKey or (startColumnKey, endColumnKey) or all null
	 */
	private List<HColumn> querySubColumns(Object rowKey, Object superColumnKey, Object columnKey,
			Object startColumnKey, Object endColumnKey)
	{
		SubSliceQuery query = HFactory.createSubSliceQuery(keyspace.keyspace, getKeySerializer(),
				getColumnSerializer(), getSubColumnSerializer(), getValueSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setSuperColumn(superColumnKey);
		
		if (columnKey != null)
			query.setColumnNames(columnKey);
		else
			query.setRange(startColumnKey, endColumnKey, false, Integer.MAX_VALUE);
		
		QueryResult<ColumnSlice> result = query.execute();
		if (result == null)
			return null;
		
		ColumnSlice slice = result.get();
		if (slice == null)
			return null;
		
		List<HColumn> columns = slice.getColumns();
		return columns;
	}
	
	/**
	 * Pass columnKey or (startColumnKey, endColumnKey) or all null
	 */
	private List<HCounterColumn> querySubCounterColumns(Object rowKey, Object superColumnKey, Object columnKey,
			Object startColumnKey, Object endColumnKey)
	{
		SubSliceCounterQuery query = HFactory.createSubSliceCounterQuery(keyspace.keyspace, getKeySerializer(),
				getColumnSerializer(), getSubColumnSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setSuperColumn(superColumnKey);
		
		if (columnKey != null)
			query.setColumnNames(columnKey);
		else
			query.setRange(startColumnKey, endColumnKey, false, Integer.MAX_VALUE);
		
		QueryResult<CounterSlice> result = query.execute();
		if (result == null)
			return null;
		
		CounterSlice slice = result.get();
		if (slice == null)
			return null;
		
		List<HCounterColumn> columns = slice.getColumns();
		return columns;
	}
	
	private List<HSuperColumn> querySuperColumns(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		SuperSliceQuery query = HFactory.createSuperSliceQuery(keyspace.keyspace, getKeySerializer(),
				getColumnSerializer(), getSubColumnSerializer(), getValueSerializer());
		query.setColumnFamily(name);
		query.setKey(rowKey);
		query.setRange(startColumnKey, endColumnKey, false, Integer.MAX_VALUE);
		
		QueryResult<SuperSlice> queryResult = query.execute();
		if (queryResult == null)
			return null;
		
		SuperSlice slice = queryResult.get();
		if (slice == null)
			return null;
		
		List<HSuperColumn> columns = slice.getSuperColumns();
		return columns;
	}
	
	private List<HCounterSuperColumn> querySuperCounterColumns(Object rowKey, Object startColumnKey, Object endColumnKey)
	{
		MultigetSuperSliceCounterQuery query = HFactory.createMultigetSuperSliceCounterQuery(keyspace.keyspace,
				getKeySerializer(), getColumnSerializer(), getSubColumnSerializer());
		query.setColumnFamily(name);
		query.setKeys(rowKey);
		query.setRange(startColumnKey, endColumnKey, false, Integer.MAX_VALUE);
		
		QueryResult<CounterSuperRows> queryResult = query.execute();
		if (queryResult == null)
			return null;
		
		CounterSuperRows rows = queryResult.get();
		if (rows == null)
			return null;
		
		Iterator<CounterSuperRow> it = rows.iterator();
		if (!it.hasNext())
			return null;
		
		CounterSuperSlice slice = it.next().getSuperSlice();
		if (slice == null)
			return null;
		
		List<HCounterSuperColumn> columns = slice.getSuperColumns();
		return columns;
	}
	
	// ////////////////////////////////////////////////////////////////////////
	
	private static AbstractSerializer getSerializer(CassandraType type)
	{
		switch (type)
		{
			case UTF8:
				return StringSerializer.get();
			case Integer:
				return IntegerSerializer.get();
			case Long:
				return LongSerializer.get();
			case TimeUUID:
				return TimeUUIDSerializer.get();
			case UUID:
				return UUIDSerializer.get();
			case Counter:
				return LongSerializer.get();
			default:
				throw new CassandraException("Something was not implemented (invalid type: " + type + ")");
		}
	}
	
	private static String getValidationClass(CassandraType type)
	{
		if (type == null)
			return null;
		
		switch (type)
		{
			case UTF8:
				return ComparatorType.UTF8TYPE.getClassName();
			case Integer:
				return ComparatorType.INTEGERTYPE.getClassName();
			case Long:
				return ComparatorType.LONGTYPE.getClassName();
			case TimeUUID:
				return ComparatorType.TIMEUUIDTYPE.getClassName();
			case UUID:
				return ComparatorType.UUIDTYPE.getClassName();
			case Counter:
				return ComparatorType.COUNTERTYPE.getClassName();
			default:
				throw new CassandraException("Something was not implemented (invalid type: " + type + ")");
		}
	}
	
	private static ComparatorType getComparatorType(CassandraType type)
	{
		if (type == null)
			return null;
		
		switch (type)
		{
			case UTF8:
				return ComparatorType.UTF8TYPE;
			case Integer:
				return ComparatorType.INTEGERTYPE;
			case Long:
				return ComparatorType.LONGTYPE;
			case TimeUUID:
				return ComparatorType.TIMEUUIDTYPE;
			case UUID:
				return ComparatorType.UUIDTYPE;
			default:
				throw new CassandraException("Something was not implemented (invalid type: " + type + ")");
		}
	}
}
