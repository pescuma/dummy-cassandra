package org.pescuma.dummycassandra;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SubSliceCounterQuery;

/**
 * Iterates over the column slice, refreshing until all qualifing columns are
 * retrieved.
 * 
 * @author thrykol
 */
class SubSliceCounterIterator<K, SN, N> implements Iterator<HCounterColumn<N>>
{
	private static final int DEFAULT_COUNT = 100;
	private SubSliceCounterQuery<K, SN, N> query;
	private Iterator<HCounterColumn<N>> iterator;
	private N start;
	private ColumnSliceFinish<N> finish;
	private boolean reversed;
	private int count = DEFAULT_COUNT;
	private int columns = 0;
	private boolean skipFirst = false;
	
	/**
	 * Constructor
	 * 
	 * @param query Base SubSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 */
	public SubSliceCounterIterator(SubSliceCounterQuery<K, SN, N> query, N start, final N finish, boolean reversed)
	{
		this(query, start, finish, reversed, DEFAULT_COUNT);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base SubSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 * @param count the amount of columns to retrieve per batch
	 */
	public SubSliceCounterIterator(SubSliceCounterQuery<K, SN, N> query, N start, final N finish, boolean reversed,
			int count)
	{
		this(query, start, new ColumnSliceFinish<N>() {
			@Override
			public N function()
			{
				return finish;
			}
		}, reversed, count);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base SubSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range. Allows for a dynamically
	 *        determined point
	 * @param reversed Whether or not the columns should be reversed
	 */
	public SubSliceCounterIterator(SubSliceCounterQuery<K, SN, N> query, N start, ColumnSliceFinish<N> finish,
			boolean reversed)
	{
		this(query, start, finish, reversed, DEFAULT_COUNT);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base SubSliceCounterQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range. Allows for a dynamically
	 *        determined point
	 * @param reversed Whether or not the columns should be reversed
	 * @param count the amount of columns to retrieve per batch
	 */
	public SubSliceCounterIterator(SubSliceCounterQuery<K, SN, N> query, N start, ColumnSliceFinish<N> finish,
			boolean reversed, int count)
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
		// only need to do another query if maximum columns were retrieved (or
		// if it is the first one)
		if (iterator == null || (!iterator.hasNext() && columns == count))
		{
			columns = 0;
			skipFirst = (iterator != null);
			iterator = doQuery();
		}
		
		return iterator.hasNext();
	}
	
	private Iterator<HCounterColumn<N>> doQuery()
	{
		query.setRange(start, finish.function(), reversed, count);
		
		QueryResult<CounterSlice<N>> queryResult = query.execute();
		if (queryResult == null)
			return emptyIterator();
		
		CounterSlice<N> slice = queryResult.get();
		if (slice == null)
			return emptyIterator();
		
		List<HCounterColumn<N>> columns = slice.getColumns();
		return columns.iterator();
	}
	
	private Iterator<HCounterColumn<N>> emptyIterator()
	{
		return new Iterator<HCounterColumn<N>>() {
			@Override
			public boolean hasNext()
			{
				return false;
			}
			
			@Override
			public HCounterColumn<N> next()
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
	public HCounterColumn<N> next()
	{
		HCounterColumn<N> column = iterator.next();
		
		// First element is start which was the last element on the previous
		// query result - skip it
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
	public interface ColumnSliceFinish<N>
	{
		
		/**
		 * Generic function for deriving a new finish point.
		 * 
		 * @return New finish point
		 */
		N function();
	}
}
