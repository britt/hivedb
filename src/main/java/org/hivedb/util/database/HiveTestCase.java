package org.hivedb.util.database;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Prepares a Derby version of the Hive global schema. Also provides factory
 * methods for domain POJOs configured with test data.
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class HiveTestCase extends DerbyTestCase {
	
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
		final Resource resource = new Resource("FOO_TABLE", new ArrayList<SecondaryIndex>());
		resource.setPartitionDimension(createEmptyPartitionDimension());
		return resource;
	}

	protected SecondaryIndex createSecondaryIndex() {
		SecondaryIndex index = new SecondaryIndex(new ColumnInfo("FOO",
				java.sql.Types.VARCHAR));
		index.setResource(createResource());
		return index;
	}
	protected SecondaryIndex createSecondaryIndex(int id) {
		SecondaryIndex index = new SecondaryIndex(id, new ColumnInfo("FOO",
				java.sql.Types.VARCHAR));
		index.setResource(createResource());
		return index;
	}

	protected Node createNode(String name) {
		return new Node("myNode",getConnectString(name), false);
	}

	protected NodeGroup createPopulatedNodeGroup() {
		NodeGroup group = createEmptyNodeGroup();
		group.add(createNode(getHiveDatabaseName()));
		return group;
	}

	protected NodeGroup createEmptyNodeGroup() {
		ArrayList<Node> nodes = new ArrayList<Node>();
		return new NodeGroup(nodes);
	}

	protected PartitionDimension createPopulatedPartitionDimension() {
		return new PartitionDimension(partitionDimensionName(), Types.INTEGER,
				createEmptyNodeGroup(), getConnectString(getHiveDatabaseName()), createResources());
	}
	protected PartitionDimension createEmptyPartitionDimension() {
		return new PartitionDimension(partitionDimensionName(), Types.INTEGER,
				createEmptyNodeGroup(), getConnectString(getHiveDatabaseName()), new ArrayList<Resource>());
	}
	protected String partitionDimensionName() {
		return "member";
	}
	protected HiveSemaphore createHiveSemaphore() {
		return new HiveSemaphore(false,54321);
	}

	protected String getHiveDatabaseName() {
		return DerbyTestCase.TEST_DB;
	}
}
