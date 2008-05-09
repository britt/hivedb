package org.hivedb.services;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.Lockable.Status;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.ContinentalSchema;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherSchema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class InstallServiceTest extends H2TestCase {
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		this.deleteDatabasesAfterEachTest = true;
		super.afterMethod();
		super.beforeMethod();
		new HiveInstaller(uri()).run();
		Hive.create(uri(), "split", Types.INTEGER, CachingDataSourceProvider.getInstance(), null);
	}
	
	@Test
	public void installANewNodeWithSchema() throws Exception {
		Schema schema = WeatherSchema.getInstance();
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
		WeatherSchema weatherSchema = WeatherSchema.getInstance();
		getService().install(weatherSchema.getName(), nodeName);
		validateSchema(weatherSchema, node);
	}
	
	@Test(expectedExceptions=HiveRuntimeException.class)
	public void tryToInstallToAReadOnlyHive() throws Exception {
		Hive hive = Hive.load(uri(), CachingDataSourceProvider.getInstance());
		hive.updateHiveStatus(Status.readOnly);
		getService().install(WeatherSchema.getInstance().getName(), "aNewNode", H2TestCase.TEST_DB, "unecessary for H2", "H2", "na", "na");
	}
	
	private void validateSchema(Schema schema, Node node) {
		for(TableInfo t :schema.getTables(node.getUri()))
			assertTrue(schema.tableExists(t.getName(), node.getUri()));
	}
	
	private String uri() {
		return getConnectString(H2TestCase.TEST_DB);
	}
	
	private Collection<Schema> getSchemata() {
		return Arrays.asList(new Schema[]{
				WeatherSchema.getInstance(),
				ContinentalSchema.getInstance()
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
