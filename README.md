dummy-cassandra
===============

A dummy interface for [Cassandra](http://cassandra.apache.org).

Built upon the shoulders of [Hector](http://hector-client.github.com/hector).

# Usage


1. Create the cluster

		CassandraCluster cluster = new CassandraCluster("test-cluster", "localhost");

1. Create the keyspaces
	
		CassandraKeyspace keyspace = cluster.addKeyspace("MyKeyspace");
	
1. Create the column families and super-column families
	
		keyspace.addColumnFamily("ColumnFamilyName", CassandraType.UTF8, CassandraType.UTF8, CassandraType.UTF8);
		
		keyspace.addSuperColumnFamily("SuperColumnFamilyName", CassandraType.UTF8, CassandraType.UTF8, CassandraType.UTF8, CassandraType.Counter);

1. Connect to the cluster and create the column families if needed

		cluster.connect();
	
1. Insert data

		keyspace.getColumnFamily("ColumnFamilyName").getRow("row-id").insert("column-name", "column-value");
	
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumn("super-column-id").incrementCounter("column-name", 1);

1. Read data

		(String) keyspace.getColumnFamily("ColumnFamilyName").getRow("row-id").getColumn("column-name");
	
		(Long) keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumn("super-column-id").getColumn("column-name");
	
1. List rows / columns

		keyspace.getColumnFamily("ColumnFamilyName").getRowKeys();
		keyspace.getColumnFamily("ColumnFamilyName").getRow("row-id").getColumnCount();
		keyspace.getColumnFamily("ColumnFamilyName").getRow("row-id").getColumnNames();
	
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRowKeys();
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumnCount();
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumnKeys();
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumn("super-column-id").getColumnCount();
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumn("super-column-id").getColumnNames();

1. Delete data

		keyspace.getColumnFamily("ColumnFamilyName").getRow("row-id").delete("column-name");
		
		keyspace.getSuperColumnFamily("SuperColumnFamilyName").getRow("row-id").getSuperColumn("super-column-id").delete("column-name");


# Dependencies

To download dependencies

	ant deps


# TODO

- Tests
- Delete rows / super-columns
- Indexes
