package org.pescuma.dummycassandra;

import java.util.Iterator;

class TransformIterable<ORIG, DEST> implements Iterable<DEST>
{
	private final Iterable<ORIG> iterable;
	private final Transformation<ORIG, DEST> transform;
	
	public TransformIterable(Iterable<ORIG> iterable, Transformation<ORIG, DEST> transform)
	{
		this.iterable = iterable;
		this.transform = transform;
	}
	
	@Override
	public Iterator<DEST> iterator()
	{
		final Iterator<ORIG> it = iterable.iterator();
		return new Iterator<DEST>() {
			@Override
			public boolean hasNext()
			{
				return it.hasNext();
			}
			
			@Override
			public DEST next()
			{
				return transform.transfor(it.next());
			}
			
			@Override
			public void remove()
			{
				it.remove();
			}
		};
	}
	
	public static interface Transformation<ORIG, DEST>
	{
		DEST transfor(ORIG obj);
	}
}
