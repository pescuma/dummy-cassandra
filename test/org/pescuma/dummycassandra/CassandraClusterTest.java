package org.pescuma.dummycassandra;

import static junit.framework.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraClusterTest {
	private CassandraCluster cluster;

	@Before
	public void setUp() throws Exception {
		// TODO Start cassandra
		// For now you need to start it manually before the tests

		cluster = new CassandraCluster("test", "localhost");
	}

	@After
	public void tearDown() throws Exception {
		// TODO Tear down cassandra 

		if (cluster.isConnected())
			cluster.disconnect();
	}

	@Test
	public void testConnect() {
		assertEquals(false, cluster.isConnected());

		cluster.connect();

		assertEquals(true, cluster.isConnected());
	}

	@Test
	public void testDisconnect() {
		cluster.connect();

		assertEquals(true, cluster.isConnected());

		cluster.disconnect();

		assertEquals(false, cluster.isConnected());
	}

	@Test
	public void testListKeyspaces() {
		assertNotNull(cluster.getKeyspaces());

		cluster.connect();

		assertNotNull(cluster.getKeyspaces());
	}

	@Test
	public void testCreateKeyspace() {
		CassandraKeyspace keyspace = cluster.addKeyspace("Test");

		assertNotNull(keyspace);

		assertSame(keyspace, cluster.getKeyspace("Test"));

		cluster.connect();

		assertSame(keyspace, cluster.getKeyspace("Test"));
	}

	@Test
	public void testCreateColumnFamily() {
		CassandraKeyspace keyspace = cluster.addKeyspace("Test");

		CassandraColumnFamily cf = keyspace.addColumnFamily("CF", CassandraType.UTF8, CassandraType.UTF8,
				CassandraType.UTF8);

		assertNotNull(cf);

		assertSame(cf, keyspace.getColumnFamily("CF"));

		cluster.connect();

		assertSame(cf, keyspace.getColumnFamily("CF"));
	}

	@Test
	public void testCounterCanOnlyBeInValue() {
		CassandraKeyspace keyspace = cluster.addKeyspace("Test");

		try {
			keyspace.addColumnFamily("CF", CassandraType.Counter, CassandraType.UTF8, CassandraType.UTF8);
			fail();
		} catch (Exception e) {
		}

		try {
			keyspace.addColumnFamily("CF", CassandraType.UTF8, CassandraType.Counter, CassandraType.UTF8);
			fail();
		} catch (Exception e) {
		}

		keyspace.addColumnFamily("CF", CassandraType.UTF8, CassandraType.UTF8, CassandraType.Counter);
	}

}
