package org.hivedb.util.database.test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.QuickCache;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class H2HiveTestCase extends H2TestCase {
	
	public H2HiveTestCase() {
		cleanupAfterEachTest = true;
	}
	
	@Override
	@BeforeClass
	protected void beforeClass() {
		if(this.getDatabaseNames() == null)
			setDatabaseNames(new ArrayList<String>());
		if(!getDatabaseNames().contains(getHiveDatabaseName())) {
			ArrayList<String> names = new ArrayList<String>();
			names.addAll(getDatabaseNames());
			names.add(getHiveDatabaseName());
			setDatabaseNames(names);
		}
		super.beforeClass();
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();
	}

	protected Collection<Resource> createResources() {
		ArrayList<Resource> resources = new ArrayList<Resource>();
		resources.add(createResource());
		return resources;
	}
	protected Resource createResource() {
		final Resource resource = new Resource("FOO", Types.INTEGER, false);
		resource.setPartitionDimension(createEmptyPartitionDimension());
		return resource;
	}
	protected SecondaryIndex createSecondaryIndex() {
		SecondaryIndex index = new SecondaryIndex("FOO",java.sql.Types.VARCHAR);
		index.setResource(createResource());
		return index;
	}
	protected SecondaryIndex createSecondaryIndex(int id) {
		SecondaryIndex index = new SecondaryIndex(id, "FOO",java.sql.Types.VARCHAR);
		index.setResource(createResource());
		return index;
	}
	protected Node createNode(String name) {
		return new Node(0, name, name, "", 0, HiveDbDialect.H2);
	}
	protected PartitionDimension createPopulatedPartitionDimension() {
		return new PartitionDimension(partitionDimensionName(), Types.INTEGER,
				new ArrayList<Node>(), getConnectString(getHiveDatabaseName()), createResources());
	}
	protected PartitionDimension createEmptyPartitionDimension() {
		return new PartitionDimension(partitionDimensionName(), Types.INTEGER,
				new ArrayList<Node>(), getConnectString(getHiveDatabaseName()), new ArrayList<Resource>());
	}
	protected String partitionDimensionName() {
		return "member";
	}
	protected HiveSemaphore createHiveSemaphore() {
		return new HiveSemaphore(false,54321);
	}
	protected String getHiveDatabaseName() {
		return H2TestCase.TEST_DB;
	}
	QuickCache quickCache = new QuickCache();
	protected Hive getOrCreateHive(final String dimensionName) {
		return quickCache.get(dimensionName, new Delay<Hive>() {
			public Hive f() {
				return  Hive.create(
						getConnectString(getHiveDatabaseName()),
						dimensionName,
						Types.INTEGER);
			}
		});
	}
}
