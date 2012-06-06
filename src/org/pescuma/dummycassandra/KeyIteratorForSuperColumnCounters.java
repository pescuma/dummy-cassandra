package org.pescuma.dummycassandra;

import java.util.Iterator;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.StringKeyIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.CounterSuperRow;
import me.prettyprint.hector.api.beans.OrderedCounterSuperRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSuperSlicesCounterQuery;

/**
 * This class returns each key in the specified Column Family as an Iterator.
 * You can use this class in a for loop without the overhead of first storing
 * each key in a large array. See StringKeyIterator for a convenience class if
 * the key is a String.
 * 
 * @author Tim Koop
 * @param <K> the type of the row key
 * @see StringKeyIterator
 */
class KeyIteratorForSuperColumnCounters<K> implements Iterable<K>
{
	private static StringSerializer stringSerializer = new StringSerializer();
	
	private static int MAX_ROW_COUNT_DEFAULT = 500;
	private int maxColumnCount = 0;
	
	private Iterator<CounterSuperRow<K, String, String>> rowsIterator = null;
	
	private RangeSuperSlicesCounterQuery<K, String, String> query = null;
	
	private K nextValue = null;
	private K lastReadValue = null;
	private K endKey;
	private boolean firstRun = true;
	
	private Iterator<K> keyIterator = new Iterator<K>() {
		@Override
		public boolean hasNext()
		{
			return nextValue != null;
		}
		
		@Override
		public K next()
		{
			K next = nextValue;
			findNext(false);
			return next;
		}
		
		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	};
	
	private void findNext(boolean fromRunQuery)
	{
		nextValue = null;
		if (rowsIterator == null)
		{
			return;
		}
		while (rowsIterator.hasNext() && nextValue == null)
		{
			CounterSuperRow<K, String, String> row = rowsIterator.next();
			lastReadValue = row.getKey();
			nextValue = lastReadValue;
		}
		if (!rowsIterator.hasNext() && nextValue == null)
		{
			runQuery(lastReadValue, endKey);
		}
	}
	
	public KeyIteratorForSuperColumnCounters(Keyspace keyspace, String columnFamily, AbstractSerializer<K> serializer)
	{
		this(keyspace, columnFamily, serializer, null, null, MAX_ROW_COUNT_DEFAULT);
	}
	
	public KeyIteratorForSuperColumnCounters(Keyspace keyspace, String columnFamily, AbstractSerializer<K> serializer,
			int maxRowCount)
	{
		this(keyspace, columnFamily, serializer, null, null, maxRowCount);
	}
	
	public KeyIteratorForSuperColumnCounters(Keyspace keyspace, String columnFamily, AbstractSerializer<K> serializer,
			K start, K end)
	{
		this(keyspace, columnFamily, serializer, start, end, MAX_ROW_COUNT_DEFAULT);
	}
	
	public KeyIteratorForSuperColumnCounters(Keyspace keyspace, String columnFamily, AbstractSerializer<K> serializer,
			K start, K end, int maxRowCount)
	{
		query = HFactory.createRangeSuperSlicesCounterQuery(keyspace, serializer, stringSerializer, stringSerializer) //
				.setColumnFamily(columnFamily) //
				.setRange(null, null, false, maxColumnCount) //
				.setRowCount(maxRowCount);
		
		endKey = end;
		runQuery(start, end);
	}
	
	private void runQuery(K start, K end)
	{
		query.setKeys(start, end);
		
		rowsIterator = null;
		QueryResult<OrderedCounterSuperRows<K, String, String>> result = query.execute();
		OrderedCounterSuperRows<K, String, String> rows = (result != null) ? result.get() : null;
		rowsIterator = (rows != null) ? rows.iterator() : null;
		
		// we'll skip this first one, since it is the same as the last one from
		// previous time we executed
		if (!firstRun && rowsIterator != null)
			rowsIterator.next();
		
		firstRun = false;
		
		if (!rowsIterator.hasNext())
		{
			nextValue = null; // all done. our iterator's hasNext() will now
			// return false;
		}
		else
		{
			findNext(true);
		}
	}
	
	@Override
	public Iterator<K> iterator()
	{
		return keyIterator;
	}
}
