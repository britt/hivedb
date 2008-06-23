package org.hivedb.util;

import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.Schema;
import org.hivedb.meta.Node;
import org.hivedb.util.HiveCreator;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.ho.yaml.Yaml;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests HiveCreator and HiveDestructor functionality
 * 
 * @author mellwanger 
 */
@Config(file="hive_default")
public class HiveCreatorTest extends HiveTest {
	
	@Test
	public void testCreateHive() throws Exception {
		validateHive(getHive(), getHiveConfigurationFile());
	}
	
	@Test
	public void testDestroyHive() throws Exception {
		HiveDestructor hiveDestructor = new HiveDestructor();
		hiveDestructor.destroy(getHive());
		for (Node node : getHive().getNodes()) {
			try {
				// We have to verify by running a select to since H2 automatically starts the mem db on getConnection
				DriverManager.getConnection(node.getUri()).prepareStatement("select * from weather_report").execute();
				throw new RuntimeException(String.format("Node %s not destroyed", node.getName()));
			} catch (SQLException ex) {
				// expected
			}
		}
		try {
			DriverManager.getConnection(getHive().getUri());
			// We have to verify by running a select to since H2 automatically starts the mem db on getConnection
			DriverManager.getConnection(getHive().getUri()).prepareStatement("select * from hive_primary_member").execute();
			throw new RuntimeException(String.format("Hive %s not destroyed", getHive().getName()));
		} catch (SQLException ex) {
			// expected
		}
		new HiveCreator(dialect).load(getHiveConfigurationFile());
	}
	
	@SuppressWarnings("unchecked")
	private void validateHive(Hive hive, String file) throws Exception {
		Map<String, Map<String, ?>> configs = (Map<String, Map<String, ?>>) Yaml.load(new FileReader(file));
		if (configs == null || configs.size() != 1) {
			throw new RuntimeException(String.format("Zero or multipe hives defined in %s", file));
		}
		validateHive(hive, configs.values().iterator().next());
	}
	
	@SuppressWarnings("unchecked")
	private void validateHive(Hive hive, Map<String, ?> config) throws Exception {
		validateDimension(hive, (Map<String, String>) config.get("dimension"));
		validateNodes(hive, (List<Map<String, ?>>) config.get("nodes"));
		validateResources(hive, (List<Map<String, ?>>) config.get("resources"));
	}
	
	private void validateDimension(Hive hive, Map<String, String> dimension) {
		String name = dimension.get("name");
		String type = dimension.get("type");
		Assert.assertEquals(hive.getPartitionDimension().getName(), name);
		Assert.assertTrue(varcharToString(JdbcTypeMapper.jdbcTypeToString(hive.getPartitionDimension().getColumnType())).equalsIgnoreCase(type));
	}
	
	@SuppressWarnings("unchecked")
	private void validateNodes(Hive hive, List<Map<String, ?>> nodes) throws Exception {
		for (Map<String, ?> node : nodes) {
			String name = (String) node.get("name");
			Assert.assertNotNull(hive.getNode(name));
			for (Map<String, String> schema : (List<Map<String, String>>) node.get("schemas")) {
				Class<? extends Schema> schemaClass = (Class<? extends Schema>) Class.forName(schema.get("class"));
				Assert.assertTrue(Schemas.getDataSchemas(hive.getNode(name).getUri()).contains(getSchemaInstance(schemaClass)));
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void validateResources(Hive hive, List<Map<String, ?>> resources) {
		if (resources != null) {
			for (Map<String, ?> resource : resources) {
				String name = (String) resource.get("name");
				String type = (String) resource.get("type");
				Assert.assertNotNull(hive.getPartitionDimension().getResource(name));
				Assert.assertTrue(varcharToString(JdbcTypeMapper.jdbcTypeToString(hive.getPartitionDimension().getResource(name).getColumnType())).equalsIgnoreCase(type));
				validateSecondaryIndexes(hive, name, (List<Map<String, String>>) resource.get("indexes"));
			}	
		}
	}
	
	private void validateSecondaryIndexes(Hive hive, String resourceName, List<Map<String, String>> indexes) {
		if (indexes != null) {
			for (Map<String, String> index : indexes) {
				String name = index.get("name");
				String type = index.get("type");
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
