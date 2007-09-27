/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.ResourceIdentifiable;

public class HiveScenarioConfigForResourceAndPartitionDimensionEntity implements HiveScenarioConfig {
	
	private Hive hive;
	private Collection<Node> dataNodes;
	public HiveScenarioConfigForResourceAndPartitionDimensionEntity(Hive hive,Collection<Node> dataNodes) {
		this.hive = hive;
		this.dataNodes = dataNodes;
	}
	
	public Hive getHive() {
		return hive;
	}

	public  ResourceIdentifiable<Object> getResourceIdentifiable() {
		return HiveScenarioMarauderClasses.getPirateConfiguration();
	}

	public Collection<Node> getDataNodes() {
		return dataNodes;
	}
}