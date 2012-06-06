package org.pescuma.dummycassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.cassandra.service.MultigetSuperSliceCounterIterator;
import me.prettyprint.cassandra.service.SliceCounterIterator;
import me.prettyprint.cassandra.service.SubSliceCounterIterator;
import me.prettyprint.cassandra.service.SubSliceIterator;
import me.prettyprint.cassandra.service.SuperSliceIterator;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.HCounterSuperColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
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
	private int pageSize = 1000;
	
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
	
	int getPageSize()
	{
		return pageSize;
	}
	
	/**
	 * @param pageSize <= 0 to disable
	 */
	void setPageSize(int pageSize)
	{
		// We can't fetch only one element, because we need to fetch the last
		// previous
		if (pageSize == 1)
			pageSize = 2;
		
		if (pageSize <= 0)
			pageSize = Integer.MAX_VALUE;
		
		this.pageSize = pageSize;
	}
	
	ColumnFamilyDefinition createColumnFamilyDefinition()
	{
		ColumnFamilyDefinition def = HFactory.createColumnFamilyDefinition(keyspace.getName(), name);
		
		String keyValidationClass = getKeyValidationClass();
		if (keyValidationClass != null)
			def.setKeyValidationClass(keyValidationClass);
		
		ComparatorType comparatorType = getColumnComparatorType();
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
	
	ComparatorType getColumnComparatorType()
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
		if (valueType == CassandraType.Counter)
		{
			return new TransformIterable<HCounterColumn, Object>(
			//
					queryCounterColumns(rowKey, startColumnKey, endColumnKey), //
					new TransformIterable.Transformation<HCounterColumn, Object>() {
						@Override
						public Object transfor(HCounterColumn obj)
						{
							return obj.getName();
						}
					});
		}
		else
		{
			return new TransformIterable<HColumn, Object>( //
					queryColumns(rowKey, startColumnKey, endColumnKey), //
					new TransformIterable.Transformation<HColumn, Object>() {
						@Override
						public Object transfor(HColumn obj)
						{
							return obj.getName();
						}
					});
		}
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
	
	private Iterable<HColumn> queryColumns(final Object rowKey, final Object startColumnKey, final Object endColumnKey)
	{
		return new Iterable<HColumn>() {
			@Override
			public Iterator<HColumn> iterator()
			{
				SliceQuery query = HFactory.createSliceQuery(keyspace.keyspace, getKeySerializer(),
						getColumnSerializer(), getValueSerializer());
				query.setColumnFamily(name);
				query.setKey(rowKey);
				
				return new ColumnSliceIterator(query, startColumnKey, endColumnKey, false, pageSize);
			}
		};
	}
	
	private Iterable<HCounterColumn> queryCounterColumns(final Object rowKey, final Object startColumnKey,
			final Object endColumnKey)
	{
		return new Iterable<HCounterColumn>() {
			@Override
			public Iterator<HCounterColumn> iterator()
			{
				SliceCounterQuery query = HFactory.createCounterSliceQuery(keyspace.keyspace, getKeySerializer(),
						getColumnSerializer());
				query.setColumnFamily(name);
				query.setKey(rowKey);
				
				return new SliceCounterIterator(query, startColumnKey, endColumnKey, false, pageSize);
			}
		};
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
		if (valueType == CassandraType.Counter)
		{
			return new TransformIterable<HCounterSuperColumn, Object>(
			//
					querySuperCounterColumns(rowKey, startColumnKey, endColumnKey), //
					new TransformIterable.Transformation<HCounterSuperColumn, Object>() {
						@Override
						public Object transfor(HCounterSuperColumn obj)
						{
							return obj.getName();
						}
					});
		}
		else
		{
			return new TransformIterable<HSuperColumn, Object>(
			//
					querySuperColumns(rowKey, startColumnKey, endColumnKey), //
					new TransformIterable.Transformation<HSuperColumn, Object>() {
						@Override
						public Object transfor(HSuperColumn obj)
						{
							return obj.getName();
						}
					});
		}
	}
	
	Iterable getSubColumnKeys(Object rowKey, Object superColumnKey)
	{
		return getSubColumnKeysSlice(rowKey, superColumnKey, null, null);
	}
	
	Iterable getSubColumnKeysSlice(Object rowKey, Object superColumnKey, Object startColumnKey, Object endColumnKey)
	{
		if (valueType == CassandraType.Counter)
		{
			return new TransformIterable<HCounterColumn, Object>(
			//
					querySubCounterColumns(rowKey, superColumnKey, startColumnKey, endColumnKey), //
					new TransformIterable.Transformation<HCounterColumn, Object>() {
						@Override
						public Object transfor(HCounterColumn obj)
						{
							return obj.getName();
						}
					});
		}
		else
		{
			return new TransformIterable<HColumn, Object>( //
					querySubColumns(rowKey, superColumnKey, startColumnKey, endColumnKey), //
					new TransformIterable.Transformation<HColumn, Object>() {
						@Override
						public Object transfor(HColumn obj)
						{
							return obj.getName();
						}
					});
		}
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
			for (HCounterColumn col : querySubCounterColumns(rowKey, superColumnKey, startColumnKey, endColumnKey))
				result.put(col.getName(), col.getValue());
		}
		else
		{
			for (HColumn col : querySubColumns(rowKey, superColumnKey, startColumnKey, endColumnKey))
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
			SubSliceCounterQuery query = HFactory.createSubSliceCounterQuery(keyspace.keyspace, getKeySerializer(),
					getColumnSerializer(), getSubColumnSerializer());
			query.setColumnFamily(name);
			query.setKey(rowKey);
			query.setSuperColumn(superColumnKey);
			query.setColumnNames(columnKey);
			
			QueryResult<CounterSlice> result = query.execute();
			if (result == null)
				return null;
			
			CounterSlice slice = result.get();
			if (slice == null)
				return null;
			
			HCounterColumn hSubColumn = (HCounterColumn) getSingleElement(slice.getColumns());
			if (hSubColumn == null)
				return null;
			
			return hSubColumn.getValue();
		}
		else
		{
			SubSliceQuery query = HFactory.createSubSliceQuery(keyspace.keyspace, getKeySerializer(),
					getColumnSerializer(), getSubColumnSerializer(), getValueSerializer());
			query.setColumnFamily(name);
			query.setKey(rowKey);
			query.setSuperColumn(superColumnKey);
			query.setColumnNames(columnKey);
			
			QueryResult<ColumnSlice> result = query.execute();
			if (result == null)
				return null;
			
			ColumnSlice slice = result.get();
			if (slice == null)
				return null;
			
			HColumn hSubColumn = (HColumn) getSingleElement(slice.getColumns());
			if (hSubColumn == null)
				return null;
			
			return hSubColumn.getValue();
		}
	}
	
	private Object getSingleElement(Iterable iterable)
	{
		Iterator it = iterable.iterator();
		
		if (!it.hasNext())
			return null;
		
		Object result = it.next();
		
		if (it.hasNext())
			throw new CassandraException("Should be only one element");
		
		return result;
	}
	
	private Iterable<HColumn> querySubColumns(final Object rowKey, final Object superColumnKey,
			final Object startColumnKey, final Object endColumnKey)
	{
		return new Iterable<HColumn>() {
			@Override
			public Iterator<HColumn> iterator()
			{
				SubSliceQuery query = HFactory.createSubSliceQuery(keyspace.keyspace, getKeySerializer(),
						getColumnSerializer(), getSubColumnSerializer(), getValueSerializer());
				query.setColumnFamily(name);
				query.setKey(rowKey);
				query.setSuperColumn(superColumnKey);
				
				return new SubSliceIterator(query, startColumnKey, endColumnKey, false, pageSize);
			}
		};
	}
	
	private Iterable<HCounterColumn> querySubCounterColumns(final Object rowKey, final Object superColumnKey,
			final Object startColumnKey, final Object endColumnKey)
	{
		return new Iterable<HCounterColumn>() {
			@Override
			public Iterator<HCounterColumn> iterator()
			{
				SubSliceCounterQuery query = HFactory.createSubSliceCounterQuery(keyspace.keyspace, getKeySerializer(),
						getColumnSerializer(), getSubColumnSerializer());
				query.setColumnFamily(name);
				query.setKey(rowKey);
				query.setSuperColumn(superColumnKey);
				
				return new SubSliceCounterIterator(query, startColumnKey, endColumnKey, false, pageSize);
			}
		};
	}
	
	private Iterable<HSuperColumn> querySuperColumns(final Object rowKey, final Object startColumnKey,
			final Object endColumnKey)
	{
		return new Iterable<HSuperColumn>() {
			@Override
			public Iterator<HSuperColumn> iterator()
			{
				SuperSliceQuery query = HFactory.createSuperSliceQuery(keyspace.keyspace, getKeySerializer(),
						getColumnSerializer(), getSubColumnSerializer(), getValueSerializer());
				query.setColumnFamily(name);
				query.setKey(rowKey);
				
				return new SuperSliceIterator(query, startColumnKey, endColumnKey, false, pageSize);
			}
		};
	}
	
	private Iterable<HCounterSuperColumn> querySuperCounterColumns(final Object rowKey, final Object startColumnKey,
			final Object endColumnKey)
	{
		return new Iterable<HCounterSuperColumn>() {
			@Override
			public Iterator<HCounterSuperColumn> iterator()
			{
				MultigetSuperSliceCounterQuery query = HFactory.createMultigetSuperSliceCounterQuery(keyspace.keyspace,
						getKeySerializer(), getColumnSerializer(), getSubColumnSerializer());
				query.setColumnFamily(name);
				query.setKeys(rowKey);
				
				return new MultigetSuperSliceCounterIterator(query, startColumnKey, endColumnKey, false, pageSize);
			}
		};
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
