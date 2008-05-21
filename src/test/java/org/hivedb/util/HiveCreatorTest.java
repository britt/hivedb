package org.hivedb.util;

import java.io.File;
import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.Schema;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.HiveCreator;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.H2TestCase;
import org.ho.yaml.Yaml;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HiveCreatorTest {
	private static String HIVE_CONFIG_FILE = "src/test/resources/hive.cfg.yml";

	
	@Test
	public void testCreateHive() throws Exception {
		HiveCreator hiveCreator = new HiveCreator();
		Hive hive = hiveCreator.load(HIVE_CONFIG_FILE);
		validateHive(hive, HIVE_CONFIG_FILE);
	}
	
	@SuppressWarnings("unchecked")
	private void validateHive(Hive hive, String file) throws Exception {
		validateHive(hive, (Map<String, Map<String, ?>>) Yaml.load(new FileReader(file)));
	}
	
	@SuppressWarnings("unchecked")
	private void validateHive(Hive hive, Map<String, Map<String, ?>> configs) throws Exception {
		for(Entry<String, Map<String, ?>> entry : configs.entrySet()) {
			Map<String, ?> config = entry.getValue();
			validateDimension(hive, (Map<String, String>) config.get("dimension"));
			validateNodes(hive, (List<Map<String, String>>) config.get("nodes"));
			validateResources(hive, (List<Map<String, ?>>) config.get("resources"));
		}
	}
	
	private void validateDimension(Hive hive, Map<String, String> config) {
		String name = config.get("name");
		String type = config.get("type");
		Assert.assertEquals(hive.getPartitionDimension().getName(), name);
		Assert.assertEquals(varcharToString(JdbcTypeMapper.jdbcTypeToString(hive.getPartitionDimension().getColumnType())), type.toUpperCase());
	}
	
	@SuppressWarnings("unchecked")
	private void validateNodes(Hive hive, List<Map<String, String>> configs) throws Exception {
		for (Map<String, String> config : configs) {
			String name = config.get("name");
			String schema = config.get("schema");
			Assert.assertNotNull(hive.getNode(name));
			Class<? extends Schema> schemaClass = (Class<? extends Schema>) Class.forName(schema);
			Assert.assertTrue(Schemas.getDataSchemas(hive.getNode(name).getUri()).contains(getSchemaInstance(schemaClass)));
		}
	}
	
	@SuppressWarnings("unchecked")
	private void validateResources(Hive hive, List<Map<String, ?>> configs) {
		if (configs != null) {
			for (Map<String, ?> config : configs) {
				String name = (String) config.get("name");
				String type = (String) config.get("type");
				Assert.assertNotNull(hive.getPartitionDimension().getResource(name));
				Assert.assertEquals(varcharToString(JdbcTypeMapper.jdbcTypeToString(hive.getPartitionDimension().getResource(name).getColumnType())), type.toUpperCase());
				validateSecondaryIndexes(hive, name, (List<Map<String, String>>) config.get("indexes"));
			}	
		}
	}
	
	private void validateSecondaryIndexes(Hive hive, String resourceName, List<Map<String, String>> configs) {
		if (configs != null) {
			for (Map<String, String> config : configs) {
				String name = config.get("name");
				String type = config.get("type");
				Assert.assertNotNull(hive.getPartitionDimension().getResource(resourceName).getSecondaryIndex(name));
				Assert.assertTrue(varcharToString(JdbcTypeMapper.jdbcTypeToString(hive.getPartitionDimension().getResource(resourceName).getSecondaryIndex(name).getColumnInfo().getColumnType())).equalsIgnoreCase(type));
			}
		}
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
	
	private String varcharToString(String type) {
		return type.equalsIgnoreCase("VARCHAR") ? "String" : type;
	}
}
