package org.hivedb.util.scenarioBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.HiveException;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.JdbcTypeMapper;

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
		hiveScenarioConfig.getHive().sync();
		return hiveScenario;
	}

	protected HiveScenario(final HiveScenarioConfig hiveScenarioConfig) throws HiveException
	{
		final Hive hive = hiveScenarioConfig.getHive();
		graphTestClasses(hiveScenarioConfig);
		Map<Class, PartitionDimension> partitionDimensionMap = configureHive(hiveScenarioConfig, hive);
		populateHive(hiveScenarioConfig, hive, partitionDimensionMap);
	}
	
	private Map<Class, Collection<Class>> primaryToResourceMap = new DebugMap<Class,Collection<Class>>();
	private Map<Class, Class> resourceToPrimaryMap = new DebugMap<Class, Class>();
	private Map<String, Class> resourceNameToClassMap = new DebugMap<String, Class>();
	public void graphTestClasses(HiveScenarioConfig hiveScenarioConfig) {
		for (Class resourceClass : hiveScenarioConfig.getResourceAndSecondaryIndexClasses()) {
			try {
				Class primaryClass = resourceClass.getMethod("getPrimaryIndexInstanceReference").getReturnType();
				if (!primaryToResourceMap.containsKey(primaryClass))
					primaryToResourceMap.put(primaryClass, new ArrayList<Class>());
				primaryToResourceMap.get(primaryClass).add(resourceClass);
				resourceToPrimaryMap.put(resourceClass, primaryClass);
			} catch (Exception e ) { throw new RuntimeException(e); }			
			resourceNameToClassMap.put(resourceClass.getSimpleName(), resourceClass);
		}	
	}

	private void populateHive(final HiveScenarioConfig hiveScenarioConfig, final Hive hive, Map<Class, PartitionDimension> partitionDimensionMap) {
		// Create a number of instances for each of the primary PartitionIndex classes, distributing them among the nodes
		Map<Class, Collection<PrimaryIndexIdentifiable>> primaryIndexInstanceMap = createPrimaryIndexInstances(hive, partitionDimensionMap, hiveScenarioConfig.getInstanceCountPerPrimaryIndex());
		// Create a number of instances for each of the secondary PartitionIndex classes.
		// Each secondary instances references a primary instance, with classes according to secondaryToPrimaryMap
		Map<String, Collection<PrimaryIndexIdentifiable>> resourceNameToPrimaryIndexInstanceMap = Transform.connectMaps(Transform.connectMaps(resourceNameToClassMap, resourceToPrimaryMap), primaryIndexInstanceMap);
		Map<Class, Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>> secondaryIndexInstances = createSecondaryIndexInstances(hive, partitionDimensionMap, resourceNameToClassMap, resourceNameToPrimaryIndexInstanceMap, hiveScenarioConfig.getInstanceCountPerSecondaryIndex());
	
		this.hive = hive;
		this.partitionDimensionMap = partitionDimensionMap;
		this.primaryIndexInstanceMap = primaryIndexInstanceMap;
		this.secondaryIndexInstances = secondaryIndexInstances;
	}

	private Map<Class, PartitionDimension> configureHive(final HiveScenarioConfig hiveScenarioConfig, final Hive hive) throws HiveException {
		final RingIteratorable<String> indexUriIterator = new RingIteratorable<String>(hiveScenarioConfig.getIndexUris(hive));
		// Create partition dimensions and their its subordinate NodeGroup, primary Node, Resources, and SecondaryIndexes
		Map<Class, PartitionDimension> partitionDimensionMap = Transform.toMap(
			new Transform.IdentityFunction<Class>(),
			new Unary<Class, PartitionDimension>() {
				public PartitionDimension f(final Class primaryClass) {
					try {
						return new PartitionDimension(
							primaryClass.getSimpleName(),
							JdbcTypeMapper.primitiveTypeToJdbcType(primaryClass.getMethod("getIdAsPrimaryIndexInstance").getReturnType()),
							new NodeGroup(Transform.map(new Unary<Node,Node>() { public Node f(Node n) {return new Node(n.getUri(), n.isReadOnly());}}, hiveScenarioConfig.getNodes(hive))),
							indexUriIterator.next(),
							Transform.map(new Unary<Class, Resource>() {
								public Resource f(Class classType) { 
									return new Resource(classType.getSimpleName(), constructSecondaryIndexesOfResource(primaryClass, classType));
								}},
								primaryToResourceMap.get(primaryClass))
						);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}},
				Arrays.asList(hiveScenarioConfig.getPrimaryClasses()));
		
		for (PartitionDimension partitionDimension : partitionDimensionMap.values())
			hive.addPartitionDimension(partitionDimension);
		
		try {
			hive.create();
		}
		catch (Exception exception)
		{
			throw new RuntimeException(exception);
		}
		return partitionDimensionMap;
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
	
	
	protected Collection<SecondaryIndex> constructSecondaryIndexesOfResource(Class primaryIndexClass, Class secondaryIndexClass) {	
		try {
			return 
				Arrays.asList(new SecondaryIndex[] {			
					new SecondaryIndex(
						new ColumnInfo(
							getNameOfSecondaryIndex(primaryIndexClass, secondaryIndexClass),											
							JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexClass.getMethod("getIdAsSecondaryIndexInstance").getReturnType())))		
				});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
			
	}

	private String getNameOfSecondaryIndex(Class primaryIndexClass, Class secondaryIndexClass) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, InstantiationException {
		Object newInstance = constructSecondaryInstance(primaryIndexClass, secondaryIndexClass);
		return (String)secondaryIndexClass.getMethod("getSecondaryIdName").invoke(newInstance);					
	}

	/**
	 *  Constructs a SecondaryIndexInstance, taking into account whethter the secondary index class
	 *  is also a primary index class (i.e. is a PrimaryAndSecondaryIndexIdentifiable)
	 *  PrimaryAndSecondaryIndexIdentifiable classes construct with no arguments, since their secondary
	 *  index is a reference from a field of the instances to its own id
	 *  SecondaryIndexIdentifiable classes construct with a PrimaryIndexIdentifiable instance, whose
	 *  id becomes the primary index key of the secondary index key
	 * @param primaryIndexClass
	 * @param secondaryIndexClass
	 * @return
	 * @throws NoSuchMethodException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public static Object constructSecondaryInstance(Class primaryIndexClass, Class secondaryIndexClass) {
		try {
			// Choose the right constructor depending on whether the secondaryIndexClass is the same as the primary index class
			Constructor constructor = secondaryIndexClass.equals(primaryIndexClass)
				? secondaryIndexClass.getConstructor(new Class[] {})
				: secondaryIndexClass.getConstructor(new Class[] {primaryIndexClass});
			Object[] args = secondaryIndexClass.equals(primaryIndexClass)
				? new Object[] {}
				: new Object[] { primaryIndexClass.getConstructor(new Class[] {}).newInstance(new Object[] {}) };			
			Object newInstance = constructor.newInstance(args);
			return newInstance;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
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
											Collection<PrimaryIndexIdentifiable> primaryInstances = resourceNameToPrimaryIndexInstanceMap.get(secondaryIndexClass.getSimpleName());
											// Make an RingIterable that iterates over the primary instancees,
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
														persistSecondaryIndex(hive, secondaryIndex, primaryIndexInstance);
														return primaryIndexInstance;
													}
														
													private SecondaryIndexIdentifiable generateAndInsertDifferentClassSecondaryIndexInstance(final Hive hive, final PartitionDimension partitionDimension, final Class secondaryIndexClass, SecondaryIndex secondaryIndex, PrimaryIndexIdentifiable primaryIndexInstance) throws Exception {
														// Construct a SecondaryIndex instance with a reference to a PrimaryIndex instance
														// In this case, the types of the primary and secondary classes are different, so make a secondary index
														// between getId() of the secondary class and the getId() of the primary class
														SecondaryIndexIdentifiable secondaryIndexInstance = (SecondaryIndexIdentifiable)secondaryIndexClass
																	.getConstructor(new Class[] {primaryIndexInstance.getClass()})
																		.newInstance(new Object[] {primaryIndexInstance});														
														hive.insertSecondaryIndexKey(secondaryIndex, secondaryIndexInstance.getIdAsSecondaryIndexInstance(), secondaryIndexInstance.getPrimaryIndexIdAsSecondaryIndexInstance());
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
	protected void persistSecondaryIndex(final Hive hive, SecondaryIndex secondaryIndex, PrimaryAndSecondaryIndexIdentifiable primaryIndexInstance) throws HiveException, SQLException {
		hive.insertSecondaryIndexKey(secondaryIndex, primaryIndexInstance.getIdAsSecondaryIndexInstance(), primaryIndexInstance.getIdAsPrimaryIndexInstance());
	}
}

