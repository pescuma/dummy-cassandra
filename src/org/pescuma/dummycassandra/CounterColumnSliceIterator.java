package org.pescuma.dummycassandra;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.SliceCounterQuery;

/**
 * Iterates over the column slice, refreshing until all qualifing columns are
 * retrieved.
 * 
 * @author thrykol
 */
class CounterColumnSliceIterator<K, N, V> implements Iterator<HCounterColumn<N>>
{
	
	private static final int DEFAULT_COUNT = 100;
	private SliceCounterQuery<K, N> query;
	private Iterator<HCounterColumn<N>> iterator;
	private N start;
	private CounterColumnSliceFinish<N> finish;
	private boolean reversed;
	private int count = DEFAULT_COUNT;
	private int columns = 0;
	
	/**
	 * Constructor
	 * 
	 * @param query Base SliceQuery to execute
	 * @param start Starting point of the range
	 * @param finish Finish point of the range.
	 * @param reversed Whether or not the columns should be reversed
	 */
	public CounterColumnSliceIterator(SliceCounterQuery<K, N> query, N start, final N finish, boolean reversed)
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
	public CounterColumnSliceIterator(SliceCounterQuery<K, N> query, N start, final N finish, boolean reversed,
			int count)
	{
		this(query, start, new CounterColumnSliceFinish<N>() {
			
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
	public CounterColumnSliceIterator(SliceCounterQuery<K, N> query, N start, CounterColumnSliceFinish<N> finish,
			boolean reversed)
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
	public CounterColumnSliceIterator(SliceCounterQuery<K, N> query, N start, CounterColumnSliceFinish<N> finish,
			boolean reversed, int count)
	{
		this.query = query;
		this.start = start;
		this.finish = finish;
		this.reversed = reversed;
		this.count = count;
		this.query.setRange(this.start, this.finish.function(), this.reversed, this.count);
	}
	
	@Override
	public boolean hasNext()
	{
		if (iterator == null)
		{
			iterator = query.execute().get().getColumns().iterator();
		}
		else if (!iterator.hasNext() && columns == count)
		{ // only need to do another query if maximum columns were retrieved
			query.setRange(start, finish.function(), reversed, count);
			iterator = query.execute().get().getColumns().iterator();
			columns = 0;
			
			// First element is start which was the last element on the previous query result - skip it
			if (iterator.hasNext())
			{
				next();
			}
		}
		
		return iterator.hasNext();
	}
	
	@Override
	public HCounterColumn<N> next()
	{
		HCounterColumn<N> column = iterator.next();
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
	 * When iterating over a CounterColumnSlice, it may be desirable to move the
	 * finish point for each query. This interface allows for a user defined
	 * function which will return the new finish point. This is especially
	 * useful for column families which have a TimeUUID as the column name.
	 */
	public interface CounterColumnSliceFinish<N>
	{
		
		/**
		 * Generic function for deriving a new finish point.
		 * 
		 * @return New finish point
		 */
		N function();
	}
}
