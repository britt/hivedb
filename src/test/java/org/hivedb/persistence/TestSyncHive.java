package org.hivedb.persistence;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
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

public class TestSyncHive extends MysqlTestCase {

	/*
	@BeforeMethod
	public void setup()
	{
		super.setUp();	
	}
	
	@Test
	public void insertPartitionDimensionAndSync() {
		try {
			final Hive hive = loadHive();
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), dataNodes);
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
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), dataNodes);
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
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), dataNodes);
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
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(), dataNodes);
			final HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, 100, 10);
			hive.sync();
			cycleSecondaryIndex(hive, hiveScenario, hiveScenarioConfig, new HiveSyncer(hive));
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	private void cyclePartitionDimension(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			// Delete a partition dimension in the hive.
			final PartitionDimension deleted = hive.deletePartitionDimension(
				Atom.getFirst(hiveScenario.getCreatedPartitionDimensions()));
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimensions()));
			
			// Add a "new" partition dimension to the syncer and sync the hive to the config
			PartitionDimension partitionDimension = new PartitionDimension(
					deleted.getName(),
					deleted.getColumnType(),
					deleted.getNodeGroup(),
					deleted.getResources());	
			hiveSyncer.addPartitionDimension(partitionDimension);
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(hiveSyncer, hive);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	private void cycleNode(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			final PartitionDimension partitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
			final Node deleted = hive.deleteNode(
				Atom.getFirst(partitionDimension.getNodeGroup().getNodes()));
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimension(partitionDimension.getName()).getNodeGroup().getNodes()));
		
			Node node = new Node(deleted.getUri(), deleted.isReadOnly());
			hiveSyncer.addNode(partitionDimension, node);
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(hiveSyncer, hive);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	private void cycleResource(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			final PartitionDimension partitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
			final Resource deleted = hive.deleteResource(
				Atom.getFirst(partitionDimension.getResources()));
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimension(partitionDimension.getName()).getResources()));
		
				
			Resource resource = new Resource(deleted.getName(), deleted.getSecondaryIndexes());
			hiveSyncer.addResource(partitionDimension, resource);
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(hiveSyncer, hive);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	
	private void cycleSecondaryIndex(final Hive hive, final HiveScenario hiveScenario, final HiveScenarioConfig hiveScenarioConfig, final HiveSyncer hiveSyncer) {
		try {
			final PartitionDimension partitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			final SecondaryIndex deleted = hive.deleteSecondaryIndex(
				Atom.getFirst(resource.getSecondaryIndexes()));
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()).getSecondaryIndexes()));
		
			SecondaryIndex secondaryIndex = new SecondaryIndex(deleted.getColumnInfo());
			hiveSyncer.addSecondaryIndex(resource, secondaryIndex);
			hiveSyncer.syncHive(hiveScenarioConfig);
			Assert.assertEquals(hiveSyncer, hive);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	
	private Hive loadHive() throws HiveException {
		return Hive.load(getConnectString());
	}


	*/

}
