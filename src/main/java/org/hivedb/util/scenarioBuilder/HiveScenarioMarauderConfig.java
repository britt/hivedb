/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;
import java.util.Map;

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
	// Classes to be used as primary indexes
	public  Class[] getPrimaryClasses() { return new Class[] { 
			HiveScenarioMarauderClasses.Pirate.class,
			HiveScenarioMarauderClasses.Buccaneer.class };}
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
	public Class[] getResourceClasses() {
		return  new Class[] {
			HiveScenarioMarauderClasses.Pirate.class, 
			HiveScenarioMarauderClasses.Buccaneer.class, 
			HiveScenarioMarauderClasses.Treasure.class,
			HiveScenarioMarauderClasses.Booty.class, 
			HiveScenarioMarauderClasses.Loot.class,
			HiveScenarioMarauderClasses.Stash.class, 
			HiveScenarioMarauderClasses.Chanty.class,
			HiveScenarioMarauderClasses.Bottle.class};
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
	HiveScenarioClassRelationships hiveScenarioClassRelationships = new HiveScenarioClassRelationships(this);
	public Map<Class, Collection<Class>> getPrimaryToResourceMap() {
		return hiveScenarioClassRelationships.getPrimaryToResourceMap();
	}
	public Map<String, Class> getResourceNameToClassMap() {
		return hiveScenarioClassRelationships.getResourceNameToClassMap();
	}
	public Map<Class, Class> getResourceToPrimaryMap() {
		return hiveScenarioClassRelationships.getResourceToPrimaryMap();
	}
}