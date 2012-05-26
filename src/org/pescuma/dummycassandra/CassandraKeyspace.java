package org.pescuma.dummycassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraKeyspace
{
	private static final Logger logger = LoggerFactory.getLogger(CassandraKeyspace.class);
	
	private final String name;
	private int replicationFactor = 1;
	private final Map<String, CassandraColumnFamily> columnFamilies = new HashMap<String, CassandraColumnFamily>();
	private final Map<String, CassandraSuperColumnFamily> superColumnFamilies = new HashMap<String, CassandraSuperColumnFamily>();
	
	Keyspace keyspace;
	
	CassandraKeyspace(String name)
	{
		this.name = name;
	}
	
	public String getName()
	{
		return name;
	}
	
	public int getReplicationFactor()
	{
		return replicationFactor;
	}
	
	public void setReplicationFactor(int replicationFactor)
	{
		this.replicationFactor = replicationFactor;
	}
	
	public CassandraColumnFamily addColumnFamily(String name, CassandraType rowKeyType, CassandraType columnKeyType,
			CassandraType valueType)
	{
		if (columnFamilies.get(name) != null)
			throw new IllegalArgumentException("ColumnFamily already registered: " + name);
		
		CassandraColumnFamily cf = new CassandraColumnFamily(this, name, rowKeyType, columnKeyType, valueType);
		columnFamilies.put(name, cf);
		return cf;
	}
	
	public CassandraSuperColumnFamily addSuperColumnFamily(String name, CassandraType rowKeyType,
			CassandraType columnKeyType, CassandraType subColumnKeyType, CassandraType valueType)
	{
		if (superColumnFamilies.get(name) != null)
			throw new IllegalArgumentException("SuperColumnFamily already registered: " + name);
		
		CassandraSuperColumnFamily cf = new CassandraSuperColumnFamily(this, name, rowKeyType, columnKeyType,
				subColumnKeyType, valueType);
		superColumnFamilies.put(name, cf);
		return cf;
	}
	
	void connect(Cluster cluster)
	{
		syncSchema(cluster);
		
		keyspace = HFactory.createKeyspace(name, cluster);
	}
	
	void shutdown()
	{
		keyspace = null;
	}
	
	private void syncSchema(Cluster cluster)
	{
		KeyspaceDefinition def = cluster.describeKeyspace(name);
		
		if (def == null)
		{
			// Create schema from start
			
			logger.trace("Keyspace " + name + " not found. Starting to create it.");
			
			ArrayList<ColumnFamilyDefinition> columns = new ArrayList<ColumnFamilyDefinition>();
			
			for (CassandraColumnFamily cf : columnFamilies.values())
				columns.add(cf.createColumnFamilyDefinition());
			
			for (CassandraSuperColumnFamily cf : superColumnFamilies.values())
				columns.add(cf.createColumnFamilyDefinition());
			
			def = HFactory.createKeyspaceDefinition(name, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, columns);
			
			cluster.addKeyspace(def, true);
			
			logger.trace("Keyspace " + name + " created.");
		}
		else
		{
			// Check if column families exists
			
			logger.trace("Keyspace " + name + " found.");
			
			Set<String> existingCFs = new HashSet<String>();
			for (ColumnFamilyDefinition cf : def.getCfDefs())
				existingCFs.add(cf.getName());
			
			ArrayList<ColumnFamilyDefinition> columns = new ArrayList<ColumnFamilyDefinition>();
			
			for (CassandraColumnFamily cf : columnFamilies.values())
				if (!existingCFs.contains(cf.getName()))
					columns.add(cf.createColumnFamilyDefinition());
			
			for (CassandraSuperColumnFamily cf : superColumnFamilies.values())
				if (!existingCFs.contains(cf.getName()))
					columns.add(cf.createColumnFamilyDefinition());
			
			if (columns.size() > 0)
			{
				for (ColumnFamilyDefinition cf : columns)
				{
					logger.trace("ColumnFamily " + cf.getName() + " of keyspace " + name
							+ " not found. Starting to create it.");
					
					cluster.addColumnFamily(cf, true);
					
					logger.trace("ColumnFamily " + cf.getName() + " of keyspace " + name + " created.");
				}
				
			}
			else
			{
				logger.trace("All column families of keyspace " + name + " already exists.");
			}
		}
	}
	
	public CassandraColumnFamily getColumnFamily(String name)
	{
		return columnFamilies.get(name);
	}
	
	public CassandraSuperColumnFamily getSuperColumnFamily(String name)
	{
		return superColumnFamilies.get(name);
	}
}
