package org.hivedb.persistence;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.HiveSyncer;
import org.hivedb.util.MysqlTestCase;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSyncHive extends DerbyTestCase {

	
	@BeforeMethod
	public void setup()
	{
		this.cleanupDbAfterEachTest = true;
	}
	
	@Test
	public void insertPartitionDimensionAndSync() {
		try {
			final Hive hive = loadHive();
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), getDataUris());
			final HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, 100, 10);
			hive.sync();
			cyclePartitionDimension(hive, hiveScenario, hiveScenarioConfig, new HiveSyncer(hive));
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void insertNodeAndSync() {
		Hive hive;
		try {
			hive = loadHive();
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), getDataUris());
			final HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, 100, 10);
			hive.sync();
			cycleNode(hive, hiveScenario, hiveScenarioConfig, new HiveSyncer(hive));
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void insertResourceAndSync() {
		try {
			final Hive hive = loadHive();
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), getDataUris());
			final HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, 100, 10);
			hive.sync();
			cycleResource(hive, hiveScenario, hiveScenarioConfig, new HiveSyncer(hive));
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void insertSecondaryIndexAndSync() {
		try {
			final Hive hive = loadHive();
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), getDataUris());
			final HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, 100, 10);
			hive.sync();
			cycleSecondaryIndex(hive, hiveScenario, hiveScenarioConfig, new HiveSyncer(hive));
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	private void cyclePartitionDimension(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();
			// Delete a partition dimension in the hive.
			final PartitionDimension deleted = hive.deletePartitionDimension(partitionDimension);
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimensions()));
			
			// Add the partition dimension back to the hive
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(partitionDimension, hive.getPartitionDimension(deleted.getName()));
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	private void cycleNode(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			final PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();
			Node node = Atom.getFirst(partitionDimension.getNodeGroup().getNodes());
			final Node deleted = hive.deleteNode(node);
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimension(partitionDimension.getName()).getNodeGroup().getNodes()));
		
			// Add the node back to the hive
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(node, hive.getPartitionDimension(partitionDimension.getName()).getNodeGroup().getNode(deleted.getName()));
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	private void cycleResource(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			final PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();
			Resource resource = Atom.getFirst(partitionDimension.getResources());
			final Resource deleted = hive.deleteResource(resource);
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimension(partitionDimension.getName()).getResources()));
		
			// Add the resource back to the hive
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(resource, hive.getPartitionDimension(partitionDimension.getName()).getResource(deleted.getName()));
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	
	private void cycleSecondaryIndex(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			final PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			SecondaryIndex secondaryIndex = Atom.getFirst(resource.getSecondaryIndexes());
			final SecondaryIndex deleted = hive.deleteSecondaryIndex(secondaryIndex);
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()).getSecondaryIndexes()));
		
			// Add the secondary index back to the hive
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(secondaryIndex, hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()).getSecondaryIndex(deleted.getName()));
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	
	private Hive loadHive() throws HiveException {
		return Hive.load(getConnectString());
	}
}
