/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;
import org.hivedb.util.InstallHiveGlobalSchema;

public class HiveScenarioMarauderConfig implements HiveScenarioConfig {
	
	private Hive hive;
	public HiveScenarioMarauderConfig(String connectString) {
		hive = InstallHiveGlobalSchema.install(connectString);
	}
	
	public Hive getHive() {
		return hive;
	}
	
	public int getInstanceCountPerPrimaryIndex() { return 10; }
	public int getInstanceCountPerSecondaryIndex() { return 100; };
	
	private List<PrimaryIndexIdentifiable> primaryInstanceIdentifiables;
	public  Collection<PrimaryIndexIdentifiable> getPrimaryInstanceIdentifiables() {
		if (primaryInstanceIdentifiables == null) {
			primaryInstanceIdentifiables = new ArrayList<PrimaryIndexIdentifiable>();
			primaryInstanceIdentifiables.add(new HiveScenarioMarauderClasses.Pirate());
		}
		return primaryInstanceIdentifiables;
	}
	
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
	@SuppressWarnings("unchecked")
	public Collection<Class<? extends ResourceIdentifiable>> getResourceClasses() {
		List<Class<? extends ResourceIdentifiable>> list = new ArrayList<Class<? extends ResourceIdentifiable>>();
		list.add(HiveScenarioMarauderClasses.Pirate.class);
		list.add(HiveScenarioMarauderClasses.Treasure.class);
		return list;
	}
	
	public Collection<String> getIndexUris(final Hive hive) {
		return Generate.create(new Generator<String>(){
			public String f() { return hive.getHiveUri(); }}, new NumberIterator(2));
	}
		// The nodes of representing the data storage databases. These may be nonunique as well.
	public Collection<Node> getDataNodes(final Hive hive) {
		return Generate.create(new Generator<Node>(){
			public Node f() { return new Node(  hive.getHiveUri(), 										
												false); }},
							  new NumberIterator(3));
	}
}