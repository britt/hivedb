package org.hivedb.util;

import static org.testng.AssertJUnit.fail;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public class IndexSchemaTestScenario {
	public static final String DIMENSION = "aSignificantDimension";
	private HiveBasicDataSource ds;
	
	public IndexSchemaTestScenario(HiveBasicDataSource ds) {
		this.ds = ds;
	}
	
	public void build() {
		PartitionDimension dimension = partitionDimension();
		IndexSchema schema = new IndexSchema(dimension);
		try {
			Hive hive = InstallHiveGlobalSchema.install(ds.getUrl());
			schema.install();
			hive.addPartitionDimension(dimension);
			Resource resource = resource();
			SecondaryIndex secondaryIndex = secondaryIndex();
			hive.addResource(dimension, resource);
			hive.addSecondaryIndex(resource, secondaryIndex);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error while installing index schema.");
		}
	}
	
	public SecondaryIndex secondaryIndex() {
		SecondaryIndex secondary = new SecondaryIndex(new ColumnInfo("werd", Types.INTEGER));
		secondary.setResource(resource());
		return secondary;
	}

	public Resource resource() {
		Resource resource = new Resource(0, "aResource", new ArrayList<SecondaryIndex>());
		resource.setPartitionDimension(partitionDimension());
		return resource;
	}

	public Node node() {
		return new Node(0, ds.getUrl(), false);
	}

	public PartitionDimension partitionDimension() {
		PartitionDimension dimension = new PartitionDimension(
				DIMENSION, Types.INTEGER);
		dimension.setIndexUri(ds.getUrl());
		dimension.setAssigner(new Assigner() {
			public Node chooseNode(Collection<Node> nodes, Object value) {
				return (Node) (nodes.iterator().hasNext() ? nodes.iterator()
						.next() : nodes.toArray()[0]);
			}
		});
		Collection<Node> nodes = new ArrayList<Node>();
		nodes.add(node());
		dimension.setNodeGroup(new NodeGroup(nodes));
		return dimension;
	}
}
