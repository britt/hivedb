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
	
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
//	@SuppressWarnings("unchecked")
//	public Collection<Class<? extends ResourceIdentifiable>> getResourceClasses() {
//		List<Class<? extends ResourceIdentifiable>> list = new ArrayList<Class<? extends ResourceIdentifiable>>();
//		list.add(HiveScenarioMarauderClasses.Pirate.class);
//		list.add(HiveScenarioMarauderClasses.Treasure.class);
//		return list;
//	}
	
	public String getHiveIndexesUri() {
		return hive.getHiveUri();
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