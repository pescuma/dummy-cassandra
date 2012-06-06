package org.pescuma.dummycassandra;

import static junit.framework.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// TODO Start cassandra
// For now you need to start it manually before the tests
public class CassandraColumnFamilyQueriesTest
{
	private CassandraCluster cluster;
	private CassandraKeyspace keyspace;
	private CassandraColumnFamily cf;
	
	@Before
	public void setUp()
	{
		cluster = new CassandraCluster("test", "localhost");
		keyspace = cluster.addKeyspace("Test");
		cf = keyspace.addColumnFamily("cf", CassandraType.UTF8, CassandraType.UTF8, CassandraType.UTF8);
		cluster.connect();
	}
	
	@After
	public void tearDown()
	{
		if (cluster.isConnected())
		{
			if (cluster.getKeyspace("Test") != null)
				cluster.removeKeyspace("Test");
			
			cluster.disconnect();
		}
	}
	
	@Test
	public void testGetColumnNamesSimple()
	{
		CassandraRow row = cf.getRow("A");
		
		for (int i = 0; i < 10; i++)
			row.insertColumn(String.format("a%03d", i), "");
		
		List<String> names = toList(row.getColumnNames());
		assertEquals(10, names.size());
		
		for (int i = 0; i < 10; i++)
			assertEquals(String.format("a%03d", i), names.get(i));
	}
	
	@Test
	public void testGetColumnNamesRangeAll()
	{
		CassandraRow row = cf.getRow("A");
		
		for (int i = 0; i < 10; i++)
			row.insertColumn(String.format("b%03d", i), "");
		
		List<String> names = toList(row.getColumnNames("a", "c"));
		assertEquals(10, names.size());
		
		for (int i = 0; i < 10; i++)
			assertEquals(String.format("b%03d", i), names.get(i));
	}
	
	@Test
	public void testGetColumnNamesRangeSmall()
	{
		CassandraRow row = cf.getRow("A");
		
		for (int i = 0; i < 100; i++)
			row.insertColumn(String.format("a%03d", i), "");
		
		List<String> names = toList(row.getColumnNames("a010", "a020"));
		assertEquals(11, names.size());
		
		for (int i = 10; i <= 20; i++)
			assertEquals(String.format("a%03d", i), names.get(i - 10));
	}
	
	@Test
	public void testGetColumnNamesRangeNone()
	{
		CassandraRow row = cf.getRow("A");
		
		for (int i = 0; i < 10; i++)
			row.insertColumn(String.format("a%03d", i), "");
		
		List<String> names = toList(row.getColumnNames("a0100", "a0101"));
		assertEquals(0, names.size());
	}
	
	@Test
	public void testGetAndDeleteColumnsWi()
	{
		cf.setPageSize(10);
		
		CassandraRow row = cf.getRow("A");
		
		for (int i = 0; i < 100; i++)
			row.insertColumn(String.format("a%03d", i), "");
		
		for (Object col : row.getColumnNames())
			row.deleteColumn(col);
		
		assertEquals(0, toList(row.getColumnNames()).size());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> List<T> toList(Iterable objs)
	{
		List<T> list = new ArrayList<T>();
		for (Object obj : objs)
			list.add((T) obj);
		return list;
	}
}
