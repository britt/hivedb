package org.hivedb.util.database;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.testng.annotations.BeforeMethod;

/**
 * Prepares a Derby version of the Hive global schema. Also provides factory
 * methods for domain POJOs configured with test data.
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public abstract class HiveTestCase extends DerbyTestCase {
	protected BasicDataSource ds;

	public HiveTestCase() {
		super();
		this.cleanupDbAfterEachTest = true;	
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		ds = new HiveBasicDataSource(getConnectString());
		new HiveInstaller(getConnectString()).run();
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

	protected Node createNode() {
		return new Node("myNode",getConnectString(), false);
	}

	protected NodeGroup createPopulatedNodeGroup() {
		NodeGroup group = createEmptyNodeGroup();
		group.add(createNode());
		group.add(createNode());
		return group;
	}

	protected NodeGroup createEmptyNodeGroup() {
		ArrayList<Node> nodes = new ArrayList<Node>();
		return new NodeGroup(nodes);
	}

	protected PartitionDimension createPopulatedPartitionDimension() {
		return new PartitionDimension(partitionDimensionName(), Types.INTEGER,
				createEmptyNodeGroup(), getConnectString(), createResources());
	}
	protected PartitionDimension createEmptyPartitionDimension() {
		return new PartitionDimension(partitionDimensionName(), Types.INTEGER,
				createEmptyNodeGroup(), getConnectString(), new ArrayList<Resource>());
	}
	protected String partitionDimensionName() {
		return "member";
	}
	protected HiveSemaphore createHiveSemaphore() {
		return new HiveSemaphore(false,54321);
	}
}
