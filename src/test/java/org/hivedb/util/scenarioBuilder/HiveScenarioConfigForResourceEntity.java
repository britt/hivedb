/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.ResourceIdentifiable;

public class HiveScenarioConfigForResourceEntity implements HiveScenarioConfig {
	
	private Hive hive;
	private Collection<Node> dataNodes;
	public HiveScenarioConfigForResourceEntity(Hive hive,Collection<Node> dataNodes) {
		this.hive = hive;
		this.dataNodes = dataNodes;
	}
	
	public Hive getHive() {
		return hive;
	}

	public  ResourceIdentifiable<Object> getResourceIdentifiable() {
		return HiveScenarioMarauderClasses.getTreasureConfiguration();
	}

	public Collection<Node> getDataNodes() {
		return dataNodes;
	}
}