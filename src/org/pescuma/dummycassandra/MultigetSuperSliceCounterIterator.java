package org.pescuma.dummycassandra;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.CounterSuperRow;
import me.prettyprint.hector.api.beans.CounterSuperRows;
import me.prettyprint.hector.api.beans.CounterSuperSlice;
import me.prettyprint.hector.api.beans.HCounterSuperColumn;
import me.prettyprint.hector.api.query.MultigetSuperSliceCounterQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Iterates over the column slice, refreshing until all qualifing columns are
 * retrieved.
 * 
 * @author thrykol
 */
class MultigetSuperSliceCounterIterator<K, SN, N> implements Iterator<HCounterSuperColumn<SN, N>>
{
	private static final int DEFAULT_COUNT = 100;
	private MultigetSuperSliceCounterQuery<K, SN, N> query;
	private Iterator<HCounterSuperColumn<SN, N>> iterator;
	private SN start;
	private ColumnSliceFinish<SN> finish;
	private boolean reversed;
	private int count = DEFAULT_COUNT;
	private int columns = 0;
	private boolean skipFirst = false;
	
	/**
	 * Constructor
	 * 
	 * @param query Base MultigetSuperSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 */
	public MultigetSuperSliceCounterIterator(MultigetSuperSliceCounterQuery<K, SN, N> query, SN start, final SN finish,
			boolean reversed)
	{
		this(query, start, finish, reversed, DEFAULT_COUNT);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base MultigetSuperSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 * @param count the amount of columns to retrieve per batch
	 */
	public MultigetSuperSliceCounterIterator(MultigetSuperSliceCounterQuery<K, SN, N> query, SN start, final SN finish,
			boolean reversed, int count)
	{
		this(query, start, new ColumnSliceFinish<SN>() {
			@Override
			public SN function()
			{
				return finish;
			}
		}, reversed, count);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base MultigetSuperSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range. Allows for a dynamically
	 *        determined point
	 * @param reversed Whether or not the columns should be reversed
	 */
	public MultigetSuperSliceCounterIterator(MultigetSuperSliceCounterQuery<K, SN, N> query, SN start,
			ColumnSliceFinish<SN> finish, boolean reversed)
	{
		this(query, start, finish, reversed, DEFAULT_COUNT);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base MultigetSuperSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range. Allows for a dynamically
	 *        determined point
	 * @param reversed Whether or not the columns should be reversed
	 * @param count the amount of columns to retrieve per batch
	 */
	public MultigetSuperSliceCounterIterator(MultigetSuperSliceCounterQuery<K, SN, N> query, SN start,
			ColumnSliceFinish<SN> finish, boolean reversed, int count)
	{
		if (count < 2)
			throw new IllegalArgumentException("At least 2 elements must be fetched each time");
		
		this.query = query;
		this.start = start;
		this.finish = finish;
		this.reversed = reversed;
		this.count = count;
	}
	
	@Override
	public boolean hasNext()
	{
		// only need to do another query if maximum columns were retrieved (or if it is the first one)
		if (iterator == null || (!iterator.hasNext() && columns == count))
		{
			columns = 0;
			skipFirst = (iterator != null);
			iterator = doQuery();
		}
		
		return iterator.hasNext();
	}
	
	private Iterator<HCounterSuperColumn<SN, N>> doQuery()
	{
		query.setRange(start, finish.function(), reversed, count);
		
		QueryResult<CounterSuperRows<K, SN, N>> queryResult = query.execute();
		if (queryResult == null)
			return emptyIterator();
		
		CounterSuperRows<K, SN, N> rows = queryResult.get();
		if (rows == null)
			return emptyIterator();
		
		Iterator<CounterSuperRow<K, SN, N>> it = rows.iterator();
		if (!it.hasNext())
			return emptyIterator();
		
		CounterSuperSlice<SN, N> slice = it.next().getSuperSlice();
		if (slice == null)
			return emptyIterator();
		
		List<HCounterSuperColumn<SN, N>> columns = slice.getSuperColumns();
		return columns.iterator();
	}
	
	private Iterator<HCounterSuperColumn<SN, N>> emptyIterator()
	{
		return new Iterator<HCounterSuperColumn<SN, N>>() {
			@Override
			public boolean hasNext()
			{
				return false;
			}
			
			@Override
			public HCounterSuperColumn<SN, N> next()
			{
				throw new NoSuchElementException();
			}
			
			@Override
			public void remove()
			{
				throw new IllegalStateException();
			}
		};
	}
	
	@Override
	public HCounterSuperColumn<SN, N> next()
	{
		HCounterSuperColumn<SN, N> column = iterator.next();
		
		// First element is start which was the last element on the previous query result - skip it
		if (skipFirst)
		{
			if (start != null && start.equals(column.getName()))
				column = iterator.next();
			
			skipFirst = false;
		}
		
		start = column.getName();
		columns++;
		
		return column;
	}
	
	@Override
	public void remove()
	{
		iterator.remove();
	}
	
	/**
	 * When iterating over a ColumnSlice, it may be desirable to move the finish
	 * point for each query. This interface allows for a user defined function
	 * which will return the new finish point. This is especially useful for
	 * column families which have a TimeUUID as the column name.
	 */
	public interface ColumnSliceFinish<SN>
	{
		
		/**
		 * Generic function for deriving a new finish point.
		 * 
		 * @return New finish point
		 */
		SN function();
	}
}
