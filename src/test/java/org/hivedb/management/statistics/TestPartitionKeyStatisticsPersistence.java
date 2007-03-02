package org.hivedb.management.statistics;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import javax.sql.DataSource;

import org.hivedb.meta.Assigner;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.Hive;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.DerbyTestCase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPartitionKeyStatisticsPersistence extends DerbyTestCase {
	private DataSource ds;
	private Hive hive;
	private ArrayList<Integer> keys;
	private PartitionDimension dimension;
	private SecondaryIndex secondaryIndex;

	@BeforeClass
	public void setUp() {
		ds = new HiveBasicDataSource(getConnectString());
		keys = new ArrayList<Integer>();
		IndexSchema schema = new IndexSchema(partitionDimension());
		try {
			new GlobalSchema(getConnectString()).install();
			new HiveSemaphoreDao(ds).create();
			schema.install();
			hive = Hive.load(getConnectString());
			dimension = partitionDimension();
			hive.addPartitionDimension(dimension);
			Resource resource = resource();
			secondaryIndex = secondaryIndex();
			hive.addResource(dimension, resource);
			hive.addSecondaryIndex(resource, secondaryIndex);

			for (int i = 0; i < 5; i++) {
				Integer key = randomIntegerKey();
				keys.add(key);
				hive.insertPrimaryIndexKey(dimension, key);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception while installing schema.");
		}
	}
	
	@Test
	public void testUpdate() {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);
		PartitionKeyStatistics frozen = null;
		PartitionKeyStatistics thawed = null;
		try {
			frozen = dao.findByPrimaryPartitionKey(dimension, keys.iterator()
					.next());
			frozen.setChildRecordCount(23);
			dao.update(frozen);
			thawed = dao.findByPrimaryPartitionKey(partitionDimension(), frozen
					.getKey());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error creating the statistics entry");
		}
		assertEquals(frozen.getChildRecordCount(), thawed.getChildRecordCount());
		assertFalse("Last updated date was not properly set", !frozen
				.getLastUpdated().equals(thawed.getLastUpdated()));

	}

	@Test
	public void testFindByPartitionKey() {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);
		PartitionKeyStatisticsBean frozen = dao.findByPrimaryPartitionKey(partitionDimension(), keys.get(0));
		assertNotNull(frozen);
	}

	@Test
	public void testIncrementChildRecords() {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);
		PartitionKeyStatistics frozen = new PartitionKeyStatisticsBean(partitionDimension(), keys
				.iterator().next(), new Date(System.currentTimeMillis()));
		frozen.setChildRecordCount(21);
		try {
			dao.update(frozen);
			dao.incrementChildRecordCount(frozen.getPartitionDimension(),
					frozen.getKey(), 2);
			PartitionKeyStatistics thawed = dao.findByPrimaryPartitionKey(frozen
					.getPartitionDimension(), frozen.getKey());
			assertEquals(frozen.getChildRecordCount() + 2, thawed
					.getChildRecordCount());
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Error creating the statistics entry");
		}
	}

	@Test
	public void testDecrementChildRecords() {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);
		PartitionKeyStatistics frozen = new PartitionKeyStatisticsBean(partitionDimension(), keys
				.iterator().next(), new Date(System.currentTimeMillis()));
		frozen.setChildRecordCount(21);
		try {
			dao.update(frozen);
			dao.decrementChildRecordCount(frozen.getPartitionDimension(),
					frozen.getKey(), 2);
			PartitionKeyStatistics thawed = dao.findByPrimaryPartitionKey(frozen
					.getPartitionDimension(), frozen.getKey());
			assertEquals(frozen.getChildRecordCount() - 2, thawed
					.getChildRecordCount());
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Error creating the statistics entry");
		}
	}
	
	@Test
	public void testFindAllByNode() throws Exception {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);

		List<PartitionKeyStatistics> stats = dao.findAllByNodeAndDimension(
				dimension,
				dimension.getNodeGroup().getNode(getConnectString()));
		assertNotNull(stats);
		assertEquals(5, stats.size());
		for(PartitionKeyStatistics s : stats) {
			assertTrue(keys.contains(s.getKey()));
			assertEquals(PartitionKeyStatisticsBean.class, s.getClass());
		}
	}

	@Test
	public void testSecondaryIndexHooks() throws Exception {
		Object key = keys.iterator().next();

		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);
		PartitionKeyStatistics frozen = dao.findByPrimaryPartitionKey(dimension, key);

		hive.insertSecondaryIndexKey(secondaryIndex, new Integer(1), key);
		hive.insertSecondaryIndexKey(secondaryIndex, new Integer(2), key);
		hive.insertSecondaryIndexKey(secondaryIndex, new Integer(3), key);

		PartitionKeyStatistics thawed = dao.findByPrimaryPartitionKey(dimension,
				frozen.getKey());

		assertEquals(frozen.getChildRecordCount() + 3, thawed
				.getChildRecordCount());
	}

	private SecondaryIndex secondaryIndex() {
		return new SecondaryIndex(new ColumnInfo("werd", Types.INTEGER));
	}

	private Resource resource() {
		return new Resource(0, "aResource", new ArrayList<SecondaryIndex>());
	}

	private Node node() {
		return new Node(0, getConnectString(), false);
	}

	private PartitionDimension partitionDimension() {
		PartitionDimension dimension = new PartitionDimension(
				"aSignificantDimension", Types.INTEGER);
		dimension.setIndexUri(getConnectString());
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

	private Integer randomIntegerKey() {
		return new Integer(new Random().nextInt());
	}

}
