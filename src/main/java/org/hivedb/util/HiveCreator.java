package org.hivedb.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.Schema;
import org.hivedb.management.HiveInstaller;
import org.hivedb.management.TerraformingHiveFactory;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.ho.yaml.Yaml;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class HiveCreator {
	private List<Schema> schemata;
	private List<Class<?>> persistableClasses;
	private String hiveUri;
	private Hive hive;
	private DataSourceProvider dataSourceProvider = CachingDataSourceProvider.getInstance();
	//private Map<Class<?>, Schema> schemaCache = new HashMap<Class<?>, Schema>();
	private Map<File, Hive> hiveCache = new HashMap<File, Hive>();
	
	static {
		try {
			Class.forName("org.h2.Driver").newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the h2 server.", e);
		}
		new HiveInstaller(String.format("jdbc:h2:mem:%s;LOCK_MODE=3", H2TestCase.TEST_DB)).run();
	}
	
	public static void main(String[] argz) throws FileNotFoundException, ClassNotFoundException, IllegalAccessException, InstantiationException, HiveLockableException {
		HiveCreator creator = new HiveCreator();
		if(argz.length == 1) {
			creator.load(argz[0]);
		} else {
			usage();
		}
	}
	
	@SuppressWarnings("unchecked")
	public Hive load(String configFile) throws FileNotFoundException, ClassNotFoundException, IllegalAccessException, InstantiationException, HiveLockableException  {
		File file = new File(configFile);
		Hive hive = hiveCache.get(file);
		if (hive == null) {
			hive = load((Map<String, Map<String, ?>>) Yaml.load(new FileReader(configFile)), file.getName().substring(0, file.getName().indexOf('.')));
			//hiveCache.put(file, hive);
		}
		return hive;
		//System.out.println(hive.toString());
	}
	
	@SuppressWarnings("unchecked")
	private Collection<Node> getNodes(List<Map<String, String>> configs) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		Collection<Node> nodes = new ArrayList<Node>();
		for (Map<String, String> config : configs) {
			String name = config.get("name");
			String schema = config.get("schema");
			Class<? extends Schema> schemaClass = (Class<? extends Schema>) Class.forName(schema);
			Node node = new Node(name, "dbName", "localhost", HiveDbDialect.H2);
			Schemas.install(getSchemaInstance(schemaClass), node.getUri());
			nodes.add(node);
		}
		return nodes;
	}
	
	private static Schema getSchemaInstance(Class<? extends Schema> schemaClass) {
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
	private Collection<Resource> getResources(List<Map<String, ?>> configs) {
		Collection<Resource> resources = new ArrayList<Resource>();
		if (configs != null) {
			for (Map<String, ?> config : configs) {
				String name = (String) config.get("name");
				String type = (String) config.get("type");
				Collection<SecondaryIndex> secondaryIndexes = getSecondaryIndexes((List<Map<String, String>>) config.get("indexes"));
				Resource resource = new Resource(name, JdbcTypeMapper.parseJdbcType(stringToVarchar(type)), true, secondaryIndexes);
				resources.add(resource);
			}	
		}
		return resources;
	}
	
	private PartitionDimension getDimension(Map<String, String> config) {
		String name = config.get("name");
		String type = config.get("type");
		PartitionDimension dimension = new PartitionDimension(name, JdbcTypeMapper.parseJdbcType(stringToVarchar(type)));
		return dimension;
	}
	
	@SuppressWarnings("unchecked")
	private Hive load(Map<String, Map<String, ?>> configs, String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException, HiveLockableException {
		for(Entry<String, Map<String, ?>> entry : configs.entrySet()) {
			Map<String, ?> config = entry.getValue();
			PartitionDimension dimension = getDimension((Map<String, String>) config.get("dimension"));
			Hive hive = Hive.create(String.format("jdbc:h2:mem:%s;LOCK_MODE=3", H2TestCase.TEST_DB), dimension.getName(), dimension.getColumnType(), dataSourceProvider, null);
			Collection<Node> nodes = getNodes((List<Map<String, String>>) config.get("nodes"));
			Collection<Resource> resources = getResources((List<Map<String, ?>>) config.get("resources"));
			hive.addNodes(nodes);
			hive.setPartitionDimension(dimension);
			for (Resource resource : resources) {
				hive.addResource(resource);
			}
			return hive;
		}
		return null;
	}
	
	private String stringToVarchar(String type) {
		return type.equalsIgnoreCase("String") ? "VARCHAR" : type;
	}
	
	public static void usage() {
		StringBuilder s = new StringBuilder("USAGE: java -cp hivedb.jar org.hivedb.util.HiveCreator CONFIG_FILE");
		System.out.println(s.toString());
	}
}
