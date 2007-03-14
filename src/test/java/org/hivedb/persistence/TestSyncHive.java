package org.hivedb.persistence;

import org.hivedb.HiveException;
import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.HiveConfiguration;
import org.hivedb.util.scenarioBuilder.Atom;
import org.hivedb.util.scenarioBuilder.Filter;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioAlternativeConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class TestSyncHive extends DerbyTestCase {
	@BeforeSuite
	public void setup()
	{
		cleanupDbAfterEachTest = true;
	}
	
	//@Test
	public void insertPartitionDimensionAndSync() {
		try {
			final Hive hive = loadHive();
			final HiveScenario hiveScenario = HiveScenario.run(new HiveScenarioAlternativeConfig(getConnectString()));
			final HiveConfiguration hiveConfiguration = new HiveConfiguration(hive);
			cyclePartitionDimension(hive, hiveScenario, hiveConfiguration);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	//@Test
	public void insertNodeAndSync() {
		Hive hive;
		try {
			hive = loadHive();
			final HiveScenario hiveScenario = HiveScenario.run(new HiveScenarioAlternativeConfig(getConnectString()));
			final HiveConfiguration hiveConfiguration = new HiveConfiguration(hive);
			cycleNode(hive, hiveScenario, hiveConfiguration);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
	
	//@Test
	public void insertResourceAndSync() {
		try {
			final Hive hive = loadHive();
			final HiveScenario hiveScenario = HiveScenario.run(new HiveScenarioAlternativeConfig(getConnectString()));
			final HiveConfiguration hiveConfiguration = new HiveConfiguration(hive);
			cycleResource(hive, hiveScenario, hiveConfiguration);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
	
	//@Test
	public void insertSecondaryIndexAndSync() {
		try {
			final Hive hive = loadHive();
			final HiveScenario hiveScenario = HiveScenario.run(new HiveScenarioAlternativeConfig(getConnectString()));
			final HiveConfiguration hiveConfiguration = new HiveConfiguration(hive);
			cycleSecondaryIndex(hive, hiveScenario, hiveConfiguration);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	private void cyclePartitionDimension(final Hive hive, final HiveScenario hiveScenario, final HiveConfiguration hiveConfiguration) {
		try {
			// Delete a partition dimension in the config and sync the hive to the config
			final PartitionDimension deleted = hiveConfiguration.deletePartitionDimension(
				Atom.getFirst(hiveScenario.getCreatedPartitionDimensions()));
			hiveConfiguration.syncHive(hive.getHiveUri());
			Assert.assertFalse(Filter.grepItemAgainstList(deleted, hive.getPartitionDimensions()));
			
			// Add a "new" partition dimension in the config and sync the hive to the config
			PartitionDimension partitionDimension = new PartitionDimension(
					deleted.getName(),
					deleted.getColumnType(),
					deleted.getNodeGroup(),
					deleted.getResources());	
			hiveConfiguration.addPartitionDimension(partitionDimension);
			hiveConfiguration.syncHive(hive.getHiveUri());
			Assert.assertEquals(hiveConfiguration, hive);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	private void cycleNode(final Hive hive, final HiveScenario hiveScenario, final HiveConfiguration hiveConfiguration) {
		try {
			final PartitionDimension partitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
			final Node deleted = hiveConfiguration.deleteNode(
				Atom.getFirst(partitionDimension.getNodeGroup().getNodes()));
			hive.sync();
				
			Node node = new Node(deleted.getUri(), deleted.isReadOnly());
			hiveConfiguration.addNode(partitionDimension, node);
			hive.sync();
			Assert.assertEquals(hiveConfiguration, hive);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	private void cycleResource(final Hive hive, final HiveScenario hiveScenario, final HiveConfiguration hiveConfiguration) {
		try {
			final PartitionDimension partitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
			final Resource deleted = hiveConfiguration.deleteResource(
				Atom.getFirst(partitionDimension.getResources()));
			hive.sync();
				
			Resource resource = new Resource(deleted.getName(), deleted.getSecondaryIndexes());
			hiveConfiguration.addResource(partitionDimension, resource);
			hive.sync();
			Assert.assertEquals(hiveConfiguration, hive);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	
	private void cycleSecondaryIndex(final Hive hive, final HiveScenario hiveScenario, final HiveConfiguration hiveConfiguration) {
		try {
			final PartitionDimension partitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			final SecondaryIndex deleted = hiveConfiguration.deleteSecondaryIndex(
				Atom.getFirst(resource.getSecondaryIndexes()));
			hive.sync();
				
			SecondaryIndex secondaryIndex = new SecondaryIndex(deleted.getColumnInfo());
			hiveConfiguration.addSecondaryIndex(resource, secondaryIndex);
			hive.sync();
			Assert.assertEquals(hiveConfiguration, hive);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}
	
	private Hive loadHive() throws HiveException {
		return Hive.load(getConnectString());
	}


	

}
