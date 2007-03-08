package org.hivedb.util.scenarioBuilder;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.HiveException;
import org.hivedb.meta.Hive;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.InstallHiveIndexSchema;

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
	
	public static HiveScenario run(HiveScenarioConfig hiveScenarioConfig) throws HiveException {
		HiveScenario hiveScenario = new HiveScenario(hiveScenarioConfig);
		hiveScenarioConfig.getHive().sync(); //Get the data that we've inserted so we can compare HiveScenario's data to the Hive's data
		return hiveScenario;
	}

	protected HiveScenario(final HiveScenarioConfig hiveScenarioConfig) throws HiveException
	{
		final Hive hive = hiveScenarioConfig.getHive();
		fill(hiveScenarioConfig, hive);
	}
	protected void fill(final HiveScenarioConfig hiveScenarioConfig, final Hive hive) throws HiveException {
		Map<Class, PartitionDimension> partitionDimensionMap = InstallHiveIndexSchema.install(hiveScenarioConfig, hive);
		populateData(hiveScenarioConfig, hive, partitionDimensionMap);
	}
	
	private void populateData(final HiveScenarioConfig hiveScenarioConfig, final Hive hive, Map<Class, PartitionDimension> partitionDimensionMap) {
		// Create a number of instances for each of the primary PartitionIndex classes, distributing them among the nodes
		Map<Class, Collection<PrimaryIndexIdentifiable>> primaryIndexInstanceMap = createPrimaryIndexInstances(hive, partitionDimensionMap, hiveScenarioConfig.getInstanceCountPerPrimaryIndex());
		// Create a number of instances for each of the secondary PartitionIndex classes.
		// Each secondary instances references a primary instance, with classes according to secondaryToPrimaryMap
		Map<String, Collection<PrimaryIndexIdentifiable>> resourceNameToPrimaryIndexInstanceMap = Transform.connectMaps(Transform.connectMaps(hiveScenarioConfig.getResourceNameToClassMap(), hiveScenarioConfig.getResourceToPrimaryMap()), primaryIndexInstanceMap);
		Map<Class, Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>> secondaryIndexInstances = createSecondaryIndexInstances(hive, partitionDimensionMap, hiveScenarioConfig.getResourceNameToClassMap(), resourceNameToPrimaryIndexInstanceMap, hiveScenarioConfig.getInstanceCountPerSecondaryIndex());
	
		this.hive = hive;
		this.partitionDimensionMap = partitionDimensionMap;
		this.primaryIndexInstanceMap = primaryIndexInstanceMap;
		this.secondaryIndexInstances = secondaryIndexInstances;
	}
	
	Hive hive;
	Map<Class, PartitionDimension> partitionDimensionMap;
	Map<Class, Collection<PrimaryIndexIdentifiable>> primaryIndexInstanceMap;
	Map<Class, Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>> secondaryIndexInstances;
	public Hive getHive() {
		return hive;
	}	
	public Collection<PartitionDimension> getCreatedPartitionDimensions()
	{
		return partitionDimensionMap.values();
	}
	public Collection<PrimaryIndexIdentifiable> getPrimaryIndexInstancesCreatedByThisPartitionDimension(PartitionDimension partitionDimension)
	{
		return primaryIndexInstanceMap.get(Transform.reverseMap(partitionDimensionMap).get(partitionDimension));
	}
	public Collection<Resource> getResourcesOfThisPartitionDimension(PartitionDimension partitionDimension)
	{
		return secondaryIndexInstances.get(Transform.reverseMap(partitionDimensionMap).get(partitionDimension)).keySet();
	}
	public Collection<SecondaryIndexIdentifiable> getSecondaryIndexInstancesForThisPartitionDimensionAndResource(PartitionDimension partitionDimension, Resource resource, SecondaryIndex secondaryIndex)
	{
		return secondaryIndexInstances.get(Transform.reverseMap(partitionDimensionMap).get(partitionDimension)).get(resource).get(secondaryIndex);
	}
	
	


	
	protected Map<Class, Collection<PrimaryIndexIdentifiable>> createPrimaryIndexInstances(final Hive hive, Map<Class, PartitionDimension> partitionDimensionMap, final int countPerPrimaryIndex)
	{	
		return Transform.toMap(
			new Transform.MapToKeyFunction<Class, PartitionDimension>(),
			new Unary<Entry<Class, PartitionDimension>, Collection<PrimaryIndexIdentifiable>>() {
				public Collection<PrimaryIndexIdentifiable> f(Entry<Class,PartitionDimension> entry) {					
					final Class partitionIndexClass = entry.getKey();
					final PartitionDimension partitionDimension = entry.getValue();			
					return generateAndInsertPrimaryIndexInstance(hive, countPerPrimaryIndex, partitionIndexClass, partitionDimension);
				}

				private Collection<PrimaryIndexIdentifiable> generateAndInsertPrimaryIndexInstance(final Hive hive, final int count, final Class primaryIndexClass, final PartitionDimension partitionDimension) {
					return Generate.create(new Generator<PrimaryIndexIdentifiable>() { 
						public PrimaryIndexIdentifiable f() {					
							try {
								PrimaryIndexIdentifiable identifiable = (PrimaryIndexIdentifiable) primaryIndexClass.getConstructor().newInstance(new Object[] {});
								persistPrimaryIndexInstance(hive, partitionDimension, identifiable);
								return identifiable;
							}
							catch ( Exception e) { throw new RuntimeException(e); }}

						},
						new NumberIterator(count));
				}
			},
			partitionDimensionMap.entrySet());
	}	
	protected void persistPrimaryIndexInstance(final Hive hive, final PartitionDimension partitionDimension, PrimaryIndexIdentifiable identifiable) throws HiveException, SQLException {
		hive.insertPrimaryIndexKey(partitionDimension, identifiable.getIdAsPrimaryIndexInstance());
	}
	
	protected Map<Class, Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>> createSecondaryIndexInstances(
		final Hive hive,
		Map<Class, PartitionDimension> partitionDimensionMap,		
		final Map<String, Class> resourceNameToClass,
		final Map<String, Collection<PrimaryIndexIdentifiable>> resourceNameToPrimaryIndexInstanceMap,
		final int countPerSecondaryIndex)
	{
		return Transform.toMap(
			new Transform.MapToKeyFunction<Class, PartitionDimension>(),
			new Unary<Entry<Class, PartitionDimension>, Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>>() {				
				public Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>> f(Entry<Class,PartitionDimension> entry) {	
					final Class primaryPartitionClass = entry.getKey();
					final PartitionDimension partitionDimension = entry.getValue();	
					
					return Transform.toMap(
						new Transform.IdentityFunction<Resource>(),
						new Unary<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>() {
							public Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>> f(final Resource resource) {													
								final Class secondaryIndexClass = resourceNameToClass.get(resource.getName());
								
								return
								Transform.toMap(
									new Transform.IdentityFunction<SecondaryIndex>(),
									new Unary<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>() {				
										public Collection<SecondaryIndexIdentifiable> f(final SecondaryIndex secondaryIndex) {
											final boolean sameClass = (secondaryIndexClass.equals(primaryPartitionClass));
											Collection<PrimaryIndexIdentifiable> primaryInstances = resourceNameToPrimaryIndexInstanceMap.get(InstallHiveIndexSchema.getSecondaryIndexIdentifiablePrototype(primaryPartitionClass, secondaryIndexClass).getResourceName());
											// Make a RingIterable that iterates over the primary instancees,
											// for countPerSecondaryIndex, but limit it it to the number of primaryInstances to prevent duplicate primary keys
											// when the secondaryInstance is the same as the primaryInstance
											final Iterable<PrimaryIndexIdentifiable> primaryPartitionIndexInstanceIterable = new RingIteratorable<PrimaryIndexIdentifiable>(
												primaryInstances,
												sameClass
													? Math.min(countPerSecondaryIndex, primaryInstances.size())
													: countPerSecondaryIndex);
											try { 
												return  Transform.map(new Unary<PrimaryIndexIdentifiable, SecondaryIndexIdentifiable>() {	
													public SecondaryIndexIdentifiable f(PrimaryIndexIdentifiable primaryIndexInstance) {	
														try {														
															return  sameClass
																? generateAndInsertSameClassSecondaryIndexInstance(hive, partitionDimension, secondaryIndexClass, secondaryIndex, (PrimaryAndSecondaryIndexIdentifiable)primaryIndexInstance)								
																: generateAndInsertDifferentClassSecondaryIndexInstance(hive, partitionDimension, secondaryIndexClass, secondaryIndex, primaryIndexInstance);
														}
														catch ( Exception e) { throw new RuntimeException(e); }
													}

													private SecondaryIndexIdentifiable generateAndInsertSameClassSecondaryIndexInstance(final Hive hive, final PartitionDimension partitionDimension, final Class secondaryIndexClass, SecondaryIndex secondaryIndex, PrimaryAndSecondaryIndexIdentifiable primaryIndexInstance) throws Exception {
														// Construct a SecondaryIndex instance with a reference to a PrimaryIndex instance
														// In this case, the types of the primary and secondary classes match, so make secondary index
														// between getSecondaryId() and getId() values										
														persistSecondaryIndex(hive, secondaryIndex, primaryIndexInstance, primaryIndexInstance);
														return primaryIndexInstance;
													}
														
													private SecondaryIndexIdentifiable generateAndInsertDifferentClassSecondaryIndexInstance(final Hive hive, final PartitionDimension partitionDimension, final Class secondaryIndexClass, SecondaryIndex secondaryIndex, PrimaryIndexIdentifiable primaryIndexInstance) throws Exception {
														// Construct a SecondaryIndex instance with a reference to a PrimaryIndex instance
														// In this case, the types of the primary and secondary classes are different, so make a secondary index
														// between getId() of the secondary class and the getId() of the primary class
														SecondaryIndexIdentifiable secondaryIndexInstance = (SecondaryIndexIdentifiable)secondaryIndexClass
																	.getConstructor(new Class[] {primaryIndexInstance.getClass()})
																		.newInstance(new Object[] {primaryIndexInstance});														
														persistSecondaryIndex(hive, secondaryIndex, secondaryIndexInstance, secondaryIndexInstance.getPrimaryIndexInstanceReference());													
														return secondaryIndexInstance;
													}	
												}, primaryPartitionIndexInstanceIterable);
											}
											catch ( Exception e) { throw new RuntimeException(e); }
									}}, resource.getSecondaryIndexes());
						}}, partitionDimension.getResources());
				}
			},
			partitionDimensionMap.entrySet());
	}
	protected void persistSecondaryIndex(final Hive hive, SecondaryIndex secondaryIndex, SecondaryIndexIdentifiable secondaryIndexInstance, PrimaryIndexIdentifiable primaryIndexInstance) throws HiveException, SQLException {
		hive.insertSecondaryIndexKey(secondaryIndex, secondaryIndexInstance.getIdAsSecondaryIndexInstance(), primaryIndexInstance.getIdAsPrimaryIndexInstance());
	}
	
	
	
}

