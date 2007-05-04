package org.hivedb.management.statistics;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.IndexSchemaTestScenario;
import org.hivedb.util.database.DerbyTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPartitionKeyStatisticsPersistence extends DerbyTestCase {
	private Collection<Integer> keys;
	private IndexSchemaTestScenario index;
	private Hive hive;
	
	@BeforeClass
	public void setUp() throws Exception{
		new HiveInstaller(getConnectString()).run();
		index = new IndexSchemaTestScenario((HiveBasicDataSource)getDataSource());
		index.build();
		try {
			hive = Hive.load(getConnectString());
		} catch (HiveException e) {
			fail("Error instantiating hive.");
		}
		
		keys = new ArrayList<Integer>();
		Random rand = new Random();
		for(int i=0; i<5; i++) {
			Integer key = new Integer(rand.nextInt());
			hive.insertPrimaryIndexKey(index.partitionDimension(), key);
			keys.add(key);
		}
	}
	
	@Test
	public void testUpdate() {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(getDataSource());
		PartitionKeyStatistics frozen = null;
		PartitionKeyStatistics thawed = null;
		try {
			frozen = dao.findByPrimaryPartitionKey(index.partitionDimension(), keys.iterator()
					.next());
			frozen.setChildRecordCount(23);
			dao.update(frozen);
			thawed = dao.findByPrimaryPartitionKey(index.partitionDimension(), frozen
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
	public void testFindByPartitionKey() throws Exception {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(getDataSource());
		PartitionKeyStatisticsBean frozen = dao.findByPrimaryPartitionKey(index.partitionDimension(), Atom.getFirst(keys));
		assertNotNull(frozen);
	}

	@Test
	public void testIncrementChildRecords() {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(getDataSource());
		PartitionKeyStatistics frozen = new PartitionKeyStatisticsBean(index.partitionDimension(), keys
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
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(getDataSource());
		PartitionKeyStatistics frozen = new PartitionKeyStatisticsBean(index.partitionDimension(), keys
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
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(getDataSource());

		List<PartitionKeyStatistics> stats = dao.findAllByNodeAndDimension(
				index.partitionDimension(),
				index.node());
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

		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(getDataSource());
		PartitionKeyStatistics frozen = dao.findByPrimaryPartitionKey(index.partitionDimension(), key);

		hive.insertSecondaryIndexKey(index.secondaryIndex(), new Integer(1), key);
		hive.insertSecondaryIndexKey(index.secondaryIndex(), new Integer(2), key);
		hive.insertSecondaryIndexKey(index.secondaryIndex(), new Integer(3), key);

		PartitionKeyStatistics thawed = dao.findByPrimaryPartitionKey(index.partitionDimension(),
				frozen.getKey());

		assertEquals(frozen.getChildRecordCount() + 3, thawed
				.getChildRecordCount());
	}
}
