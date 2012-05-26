package org.pescuma.dummycassandra;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraCluster {

	private final String name;
	private final String host;
	private final int port;
	private final Map<String, CassandraKeyspace> keyspaces = new HashMap<String, CassandraKeyspace>();

	private Cluster cluster;

	public CassandraCluster(String name, String host, int port) {
		this.name = name;
		this.host = host;
		this.port = port;
	}

	public CassandraCluster(String name, String host) {
		this(name, host, 9160);
	}

	public CassandraKeyspace addKeyspace(String name) {
		if (keyspaces.get(name) != null)
			throw new IllegalArgumentException("Keyspace already registered: " + name);

		CassandraKeyspace keyspace = new CassandraKeyspace(name);
		keyspaces.put(name, keyspace);
		return keyspace;
	}

	public void connect() {
		cluster = HFactory.getOrCreateCluster(name, host + ":" + port);

		for (CassandraKeyspace keyspace : keyspaces.values())
			keyspace.connect(cluster);
	}

	public void disconnect() {
		if (!isConnected())
			throw new IllegalStateException("You have to be connected to be able to disconnect");

		for (CassandraKeyspace keyspace : keyspaces.values())
			keyspace.shutdown();

		HFactory.shutdownCluster(cluster);

		cluster = null;
	}

	public boolean isConnected() {
		return cluster != null;
	}

	public CassandraKeyspace getKeyspace(String kesypace) {
		return keyspaces.get(kesypace);
	}

	public Iterable<CassandraKeyspace> getKeyspaces() {
		return keyspaces.values();
	}

}
