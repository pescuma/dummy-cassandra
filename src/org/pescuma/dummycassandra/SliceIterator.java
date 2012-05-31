package org.pescuma.dummycassandra;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Iterates over the column slice, refreshing until all qualifing columns are
 * retrieved.
 * 
 * @author thrykol
 */
class SliceIterator<K, N, V> implements Iterator<HColumn<N, V>>
{
	private static final int DEFAULT_COUNT = 100;
	private SliceQuery<K, N, V> query;
	private Iterator<HColumn<N, V>> iterator;
	private N start;
	private ColumnSliceFinish<N> finish;
	private boolean reversed;
	private int count = DEFAULT_COUNT;
	private int columns = 0;
	private boolean skipFirst = false;
	
	/**
	 * Constructor
	 * 
	 * @param query Base SliceQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 */
	public SliceIterator(SliceQuery<K, N, V> query, N start, final N finish, boolean reversed)
	{
		this(query, start, finish, reversed, DEFAULT_COUNT);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base SliceQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 * @param count the amount of columns to retrieve per batch
	 */
	public SliceIterator(SliceQuery<K, N, V> query, N start, final N finish, boolean reversed, int count)
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
	 * @param query Base SliceQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range. Allows for a dynamically
	 *        determined point
	 * @param reversed Whether or not the columns should be reversed
	 */
	public SliceIterator(SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed)
	{
		this(query, start, finish, reversed, DEFAULT_COUNT);
	}
	
	/**
	 * Constructor
	 * 
	 * @param query Base SliceQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range. Allows for a dynamically
	 *        determined point
	 * @param reversed Whether or not the columns should be reversed
	 * @param count the amount of columns to retrieve per batch
	 */
	public SliceIterator(SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed, int count)
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
	
	private Iterator<HColumn<N, V>> doQuery()
	{
		query.setRange(start, finish.function(), reversed, count);
		
		QueryResult<ColumnSlice<N, V>> queryResult = query.execute();
		if (queryResult == null)
			return emptyIterator();
		
		ColumnSlice<N, V> slice = queryResult.get();
		if (slice == emptyIterator())
			return emptyIterator();
		
		List<HColumn<N, V>> columns = slice.getColumns();
		return columns.iterator();
	}
	
	private Iterator<HColumn<N, V>> emptyIterator()
	{
		return new Iterator<HColumn<N, V>>() {
			@Override
			public boolean hasNext()
			{
				return false;
			}
			
			@Override
			public HColumn<N, V> next()
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
	public HColumn<N, V> next()
	{
		HColumn<N, V> column = iterator.next();
		
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
