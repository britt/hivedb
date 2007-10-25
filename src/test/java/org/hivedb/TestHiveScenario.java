package org.hivedb;

import java.sql.Types;
import java.util.Collection;

import org.hivedb.meta.Node;
import org.hivedb.util.PersisterImpl;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfigForResourceAndPartitionDimensionEntity;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfigForResourceEntity;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderClasses;
import org.hivedb.util.scenarioBuilder.HiveScenarioTest;

public class TestHiveScenario {
	
	String hiveUri;
	Collection<Node> dataNodes;
	public TestHiveScenario(String hiveUri, Collection<Node> dataNodes) {
		this.hiveUri = hiveUri;
		this.dataNodes = dataNodes;
	}
	
	public void testResourceEntity() {
		new HiveScenarioTest(
						new HiveScenarioConfigForResourceEntity(
								Hive.create(hiveUri,
										HiveScenarioMarauderClasses.getTreasureConfiguration().getPartitionDimensionName(),
										Types.INTEGER),
								dataNodes))
									.performTest(1,2, new PersisterImpl());
	}
	public void testResourceAndPartitionDimensionEntity() {
		new HiveScenarioTest(
						new HiveScenarioConfigForResourceAndPartitionDimensionEntity(
								Hive.create(hiveUri,
										HiveScenarioMarauderClasses.getPirateConfiguration().getPartitionDimensionName(),
										Types.INTEGER),
								dataNodes))
									.performTest(2,2, new PersisterImpl());
	}
}