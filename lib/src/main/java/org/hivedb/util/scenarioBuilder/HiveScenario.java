package org.hivedb.util.scenarioBuilder;

import java.util.Collection;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.util.GenerateHiveIndexKeys;
import org.hivedb.util.InstallHiveIndexSchema;
import org.hivedb.util.PersisterImpl;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

/**
 *
 * @author andylikuski Andy Likuski
 * 
 *  Given a hive URI, index URIs, and data nodes, this class contructs 
 *  partition dimensions, node groups, primary indexes, resources, and secondary index,
 *  and also creates sample object instances and inserts their ids into the index tables.
 *  The meta data is set to the hive, so that the result of constructing this class
 *  is a full hive and populated index tables. The class instance also offers a number
 *  of methods that return the meta data and instances created in order for tests to
 *  validate the persisted data.
 *  
 *  There are serveral configuration parameters listed as constants at the top of the class
 *  which are used to determine what classes are modeled and how many instances are created.
 *  You may alter these or sublcass HiveScenarioConfig to test your own classes. HiveScenario
 *  expects the classes to conform to its Identifiable interface. To test your classes subclass
 *  them and implement the Identifiable interface. The Identifiable interface allows a class
 *  to function as a primary index key and as a single secondary index key. If you want a class
 *  to only function as a primary index class you can implement PrimaryIndexIdentifiable and
 *  to only function as a resource/secondary index class implement SecondaryIndexIdentifiable.
 *  
 *  Primary Index classes must have a no argument contructor (for use by reflection)
 *  Secondary Index classes must have a 1 argument contructor (for use by reflection) whose
 *  arguement is the instance to be used as the primary index key (see SecondaryIndexidentifiable)
 */
public class HiveScenario {
	
	public static HiveScenario run(HiveScenarioConfig hiveScenarioConfig, int primaryIndexInstanceCount, int resourceInstanceCount) throws HiveException {
		HiveScenario hiveScenario = new HiveScenario(hiveScenarioConfig, primaryIndexInstanceCount, resourceInstanceCount);
		return hiveScenario;
	}

	private int primaryIndexInstanceCount;
	private int resourceInstanceCount;
	protected HiveScenario(final HiveScenarioConfig hiveScenarioConfig, int primaryIndexInstanceCount, int resourceInstanceCount)
	{
		final Hive hive = hiveScenarioConfig.getHive();
		this.primaryIndexInstanceCount = primaryIndexInstanceCount;
		this.resourceInstanceCount = resourceInstanceCount;
		fill(hiveScenarioConfig, hive);
	}
	private void fill(final HiveScenarioConfig hiveScenarioConfig, final Hive hive) {
		PartitionDimension partitionDimension = InstallHiveIndexSchema.install(hiveScenarioConfig, hive);
		this.partitionDimension = partitionDimension;
		populateData(hiveScenarioConfig, hive, partitionDimension);
	}
	private void populateData(final HiveScenarioConfig hiveScenarioConfig, final Hive hive, PartitionDimension partitionDimension) {
		GenerateHiveIndexKeys generateHiveIndexKeys = new GenerateHiveIndexKeys(new PersisterImpl(), primaryIndexInstanceCount, resourceInstanceCount);
		Collection<PrimaryIndexIdentifiable> primaryIndexIdentifiables = 
			generateHiveIndexKeys.createPrimaryIndexInstances(hive, hiveScenarioConfig);
		
		Map<ResourceIdentifiable, Collection<ResourceIdentifiable>> resourceIdentifiableMap
			= generateHiveIndexKeys.createSecondaryIndexInstances(hive, hiveScenarioConfig, primaryIndexIdentifiables);
	
		this.hive = hive;
		this.primaryIndexIdentifiables = primaryIndexIdentifiables;
		this.resourceIdentifiableMap = resourceIdentifiableMap;
	}
	
	Hive hive;
	PartitionDimension partitionDimension;
	Collection<PrimaryIndexIdentifiable>  primaryIndexIdentifiables;
	Map<ResourceIdentifiable, Collection<ResourceIdentifiable>> resourceIdentifiableMap;
	public Hive getHive() {
		return hive;
	}	
	public PartitionDimension getCreatedPartitionDimension()
	{
		return partitionDimension;
	}
	public Collection<PrimaryIndexIdentifiable> getPrimaryIndexInstancesCreatedByThisPartitionDimension(PartitionDimension partitionDimension)
	{
		return primaryIndexIdentifiables;
	}

	public Collection<ResourceIdentifiable> getResourceIdentifiableInstancesForThisResource(final Resource resource) {
	
		return resourceIdentifiableMap.get(
			Filter.grepSingle(new Predicate<ResourceIdentifiable>() {
				public boolean f(ResourceIdentifiable resourceIdentifiable) {
					return resourceIdentifiable.getResourceName().equals(resource.getName());
				}},
				resourceIdentifiableMap.keySet())
		);
	}
	
	public Collection<Resource> getResourcesOfThisPartitionDimension(PartitionDimension partitionDimension) {
		return Transform.map(
				new Unary<ResourceIdentifiable, Resource>() {
					public Resource f(ResourceIdentifiable resourceIdentifiable) {
						
						return hive.getPartitionDimension(getPartitionDimensionName(resourceIdentifiable)).getResource(resourceIdentifiable.getResourceName());
					}
				},
				resourceIdentifiableMap.keySet());
	}
	private String getPartitionDimensionName(ResourceIdentifiable resourceIdentifiable) {
		return resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName();
	}
}

