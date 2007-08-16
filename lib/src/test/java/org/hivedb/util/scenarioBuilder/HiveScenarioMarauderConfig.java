/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveScenarioMarauderConfig implements HiveScenarioConfig {
	
	private Hive hive;
	private Collection<Node> dataNodes;
	private Collection<String> dataNodeUris;
	public HiveScenarioMarauderConfig(String connectString, Collection<String> dataNodeUris) {
		hive = Hive.load(connectString);
		this.dataNodeUris = dataNodeUris;
	}
	
	public Hive getHive() {
		return hive;
	}
	
	private PrimaryIndexIdentifiable primaryIndexIdentifiables;
	public  PrimaryIndexIdentifiable getPrimaryIndexIdentifiable() {
		if (primaryIndexIdentifiables == null)
			primaryIndexIdentifiables = new HiveScenarioMarauderClasses.Pirate();
		return primaryIndexIdentifiables;
	}
	
	public String getHiveIndexesUri() {
		return hive.getUri();
	}
	
	public Collection<Node> getDataNodes() {
		if (dataNodes == null)
			dataNodes = Transform.map(new Unary<String, Node>() {
				public Node f(String dataNodeUri) {
					return new Node("daveyJonesLocker",dataNodeUri,false);	
				}},
				dataNodeUris);
		return dataNodes;
	}

}