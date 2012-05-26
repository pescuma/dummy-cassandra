package org.pescuma.dummycassandra;

public class CassandraException extends RuntimeException
{
	private static final long serialVersionUID = -343263596600678146L;
	
	public CassandraException()
	{
		super();
	}
	
	public CassandraException(String arg0, Throwable arg1)
	{
		super(arg0, arg1);
	}
	
	public CassandraException(String arg0)
	{
		super(arg0);
	}
	
	public CassandraException(Throwable arg0)
	{
		super(arg0);
	}
}
