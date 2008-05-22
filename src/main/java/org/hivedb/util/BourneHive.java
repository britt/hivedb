package org.hivedb.util;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.Schema;
import org.hivedb.meta.Node;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.H2TestCase;

/**
 * Helper class to systematically create a hive from configuration maps
 * 
 * @author mellwanger
 */
public class BourneHive {
	private Hive hive;
	
	public Hive getHive() {
		return hive;
	}
	
	public BourneHive(Map<String, String> dimension, DataSourceProvider dataSourceProvider) {
		String name = dimension.get("name");
		int type = JdbcTypeMapper.parseJdbcType(stringToVarchar(dimension.get("type")));
		hive = Hive.create(String.format("jdbc:h2:mem:%s;LOCK_MODE=3", H2TestCase.TEST_DB), name, type, dataSourceProvider, null);
	}
	
	public BourneHive addNodes(List<Map<String, String>> configs) {
		for (Map<String, String> config : configs) {
			String name = config.get("name");
			String schema = config.get("schema");
			try {
				hive.addNode(new Node(name, String.format("%s%s", name, ";LOCK_MODE=3"), "mem", HiveDbDialect.H2));
			} catch (HiveLockableException ex) {
				throw new RuntimeException(ex);
			}
			installSchema(hive.getNode(name), schema);
		}
		return this;
	}
	
	@SuppressWarnings("unchecked")
	public BourneHive addResources(List<Map<String, ?>> configs) {
		if (configs != null) {
			for (Map<String, ?> config : configs) {
				String name = (String) config.get("name");
				String type = (String) config.get("type");
				Collection<SecondaryIndex> secondaryIndexes = getSecondaryIndexes((List<Map<String, String>>) config.get("indexes"));
				try {
					hive.addResource(new Resource(name, JdbcTypeMapper.parseJdbcType(stringToVarchar(type)), true, secondaryIndexes));
				} catch (HiveLockableException ex) {
					throw new RuntimeException(ex);
				}
			}	
		}
		return this;
	}
	
	private Collection<SecondaryIndex> getSecondaryIndexes(List<Map<String, String>> configs) {
		Collection<SecondaryIndex> secondaryIndexes = new ArrayList<SecondaryIndex>();
		if (configs != null) {
			for (Map<String, String> config : configs) {
				String name = config.get("name");
				String type = config.get("type");
				SecondaryIndex secondaryIndex = new SecondaryIndex(name, JdbcTypeMapper.parseJdbcType(stringToVarchar(type)));
				secondaryIndexes.add(secondaryIndex);
			}
		}
		return secondaryIndexes;
	}
	
	@SuppressWarnings("unchecked")
	private void installSchema(Node node, String schema) {
		Class<? extends Schema> schemaClass = null;
		try {
			schemaClass = (Class<? extends Schema>) Class.forName(schema);
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex);
		}
		Schemas.install(getSchemaInstance(schemaClass), node.getUri());
	}
	
	private Schema getSchemaInstance(Class<? extends Schema> schemaClass) {
		try {
			return (Schema) schemaClass.getMethod("getInstance", new Class[] {}).invoke(null, new Object[] {});
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	private String stringToVarchar(String type) {
		return type.equalsIgnoreCase("String") ? "VARCHAR" : type;
	}	
}
