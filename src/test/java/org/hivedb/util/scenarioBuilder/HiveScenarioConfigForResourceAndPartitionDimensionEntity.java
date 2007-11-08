/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.Node;

public class HiveScenarioConfigForResourceAndPartitionDimensionEntity implements SingularHiveConfig {
	
	private Hive hive;
	private Collection<Node> dataNodes;
	public HiveScenarioConfigForResourceAndPartitionDimensionEntity(Hive hive,Collection<Node> dataNodes) {
		this.hive = hive;
		this.dataNodes = dataNodes;
	}
	
	public Hive getHive() {
		return hive;
	}

	public  EntityConfig getEntityConfig() {
		return HiveScenarioMarauderClasses.getPirateConfiguration();
	}

	public Collection<Node> getDataNodes() {
		return dataNodes;
	}
}