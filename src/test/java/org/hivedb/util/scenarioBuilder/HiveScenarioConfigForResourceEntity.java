/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.Node;

public class HiveScenarioConfigForResourceEntity implements SingularHiveConfig {
	
	private Hive hive;
	private Collection<Node> dataNodes;
	public HiveScenarioConfigForResourceEntity(Hive hive,Collection<Node> dataNodes) {
		this.hive = hive;
		this.dataNodes = dataNodes;
		try {
			hive.addNodes(this.dataNodes);
		} catch (HiveReadOnlyException e) {
			throw new HiveRuntimeException(e.getMessage(),e);
		}
	}
	
	public Hive getHive() {
		return hive;
	}

	public  EntityConfig getEntityConfig() {
		return HiveScenarioMarauderClasses.getTreasureConfiguration();
	}

	public Collection<Node> getDataNodes() {
		return dataNodes;
	}
}