package org.hivedb.services;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.Lockable.Status;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.ContinentalSchema;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherSchema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class InstallServiceTest extends H2TestCase {
	
	@BeforeMethod
	public void setup() throws Exception {
		new HiveInstaller(uri()).run();
		Hive.create(uri(), "split", Types.INTEGER, CachingDataSourceProvider.getInstance(), null);
	}
	
	@Test
	public void installANewNodeWithSchema() throws Exception {
		Schema schema = new WeatherSchema();
		String nodeName = "aNewNode";
		getService().install(schema.getName(), nodeName, H2TestCase.TEST_DB, "unecessary for H2", "H2", "na", "na");
		validateSchema(schema, Hive.load(uri(), CachingDataSourceProvider.getInstance()).getNode(nodeName));
	}
	
	@Test
	public void installASchemaOnAnExistingNode() throws Exception {
		Hive hive = Hive.load(uri(), CachingDataSourceProvider.getInstance());
		String nodeName = "anExistingNode";
		Node node = new Node(nodeName, H2TestCase.TEST_DB, "unecessary", HiveDbDialect.H2);
		hive.addNode(node);
		WeatherSchema weatherSchema = new WeatherSchema();
		getService().install(weatherSchema.getName(), nodeName);
		validateSchema(weatherSchema, node);
	}
	
	@Test(expectedExceptions=HiveRuntimeException.class)
	public void tryToInstallToAReadOnlyHive() throws Exception {
		Hive hive = Hive.load(uri(), CachingDataSourceProvider.getInstance());
		hive.updateHiveStatus(Status.readOnly);
		getService().install(new WeatherSchema().getName(), "aNewNode", H2TestCase.TEST_DB, "unecessary for H2", "H2", "na", "na");
	}
	
	private void validateSchema(Schema schema, Node node) {
		schema.setUri(node.getUri());
		for(TableInfo t :schema.getTables())
			assertTrue(schema.tableExists(t.getName()));
	}
	
	private String uri() {
		return getConnectString(H2TestCase.TEST_DB);
	}
	
	private Collection<Schema> getSchemata() {
		return Arrays.asList(new Schema[]{
				new WeatherSchema(),
				new ContinentalSchema()
		});
	}
	
	private InstallServiceImpl getService() {
		return new InstallServiceImpl(getSchemata(), Hive.load(uri(), CachingDataSourceProvider.getInstance()));
	}

	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{H2TestCase.TEST_DB});
	}
	
}
