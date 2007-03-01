package org.hivedb.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
	
	public static class HiveScenarioConfig {
		protected int getInstanceCountPerPrimaryIndex() { return 10; }
		protected int getInstanceCountPerSecondaryIndex() { return 100; };
		// Classes to be used as primary indexes
		protected  Class[] getPrimaryClasses() { return new Class[] { Pirate.class, Buccaneer.class };}
		// Classes to be used as resources and secondary indexes.
		// If the classes are also primary indexes, then the secondary index created will be
		// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
		// If the classes are no also primary classes, then the secondary index created will be
		// the class's id which references the id of another class (an inter-class reference)
		protected Class[] getResourceAndSecondaryIndexClasses() {
			return  new Class[] {
				Pirate.class, Buccaneer.class, Treasure.class, Booty.class, Loot.class, Stash.class, Chanty.class, Bottle.class};
		}
	}
	 
	private HiveScenarioConfig hiveScenarioConfig;
	private Map<Class, Collection<Class>> primaryToResourceMap = new DebugMap<Class,Collection<Class>>();
	private Map<Class, Class> resourceToPrimaryMap = new DebugMap<Class, Class>();
	private Map<String, Class> resourceNameToClassMap = new DebugMap<String, Class>();
	public void init() {
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
	
	public static HiveScenario buildAndSyncHive(final Hive hive, HiveScenarioConfig hiveScenarioConfig) throws HiveException {
		HiveScenario hiveScenario = 
			new HiveScenario(
				hiveScenarioConfig,
				hive,
				Generate.create(new Generator<String>(){
					public String f() { return hive.getHiveUri(); }}, new NumberIterator(2)),
				Generate.create(new Generator<Node>(){
					public Node f() { return new Node(  hive.getHiveUri(), 
														false); }},
									  new NumberIterator(3))
			);
		
	
		hive.sync();
		return hiveScenario;
	}
	public static HiveScenario buildAndSyncHive(final Hive hive) throws HiveException {
		return buildAndSyncHive(hive, new HiveScenarioConfig());
	}

	public HiveScenario(HiveScenarioConfig hiveScenarioConfig, final Hive hive, final Collection<String> indexUris, final Collection<Node> dataNodes) throws HiveException
	{
		this.hiveScenarioConfig = hiveScenarioConfig;
		init();
		
		final RingIteratorable<String> indexUriIterator = new RingIteratorable<String>(indexUris);
		// Create partition dimensions and their its subordinate NodeGroup, primary Node, Resources, and SecondaryIndexes
		Map<Class, PartitionDimension> partitionDimensionMap = Transform.toMap(
			new Transform.IdentityFunction<Class>(),
			new Unary<Class, PartitionDimension>() {
				public PartitionDimension f(final Class primaryClass) {
					try {
						return new PartitionDimension(
							primaryClass.getSimpleName(),
							JdbcTypeMapper.primitiveTypeToJdbcType(primaryClass.getMethod("getIdAsPrimaryIndexInstance").getReturnType()),
							new NodeGroup(Transform.map(new Unary<Node,Node>() { public Node f(Node n) {return new Node(n.getUri(),n.isReadOnly());}},dataNodes)),
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
								hive.insertPrimaryIndexKey(partitionDimension, identifiable.getIdAsPrimaryIndexInstance());
								return identifiable;
							}
							catch ( Exception e) { throw new RuntimeException(e); }}},
						new NumberIterator(count));
				}
			},
			partitionDimensionMap.entrySet());
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
														hive.insertSecondaryIndexKey(secondaryIndex, primaryIndexInstance.getIdAsSecondaryIndexInstance(), primaryIndexInstance.getIdAsPrimaryIndexInstance());
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
	// A primary index classes
	protected static class Pirate implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		public Pirate() { 
			id = ++idGenerator;
			pirateName = "name"+id; }		
		int id;
		String pirateName;
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return this; }
		public String getIdAsSecondaryIndexInstance() {	return pirateName;}
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return id; } // self referencing
		public String getSecondaryIdName()
		{
			return "name";
		}	
	}
	protected static class Buccaneer implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		public Buccaneer() { 
			id = new Integer(++idGenerator).toString();
			buccaneerName = "name"+id;};
		String id;
		String buccaneerName;
		public String getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return this; }
		public String getIdAsSecondaryIndexInstance() {	return buccaneerName; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return id; } // self referencing
		public String getSecondaryIdName()
		{
			return "name";
		}
	}
	
	
	// Resource and Secondary index classes that test all possible secondary index id types
	protected static class Treasure implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		int id;
		Pirate primaryResource;
		public Treasure(Pirate primaryResource) {
			id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); } 
		public String getSecondaryIdName()
		{
			return "pirate_id";
		}
	}	                      
	protected static class Booty implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static long idGenerator = 2^32;
		long id;
		Pirate primaryResource;
		public Booty(Pirate primaryResource) {
			id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Long getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Long getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getSecondaryIdName()
		{
			return "pirate_id";
		}
	}
	protected static class Loot implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		int id;
		Pirate primaryResource;
		public Loot(Pirate primaryResource) {
			this.id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		public String getSecondaryIdName()
		{
			return "pirate_id";
		}
	}
	protected static class Stash implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static double idGenerator = 0d;
		double id;
		Buccaneer primaryResource;
		public Stash(Buccaneer primaryResource) {
			idGenerator += 1.1;
			this.id = idGenerator;
			this.primaryResource = primaryResource;
		}
		public Double getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public Double getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		public String getSecondaryIdName()
		{
			return "bucaneer_id";
		}
	}
	protected static class Chanty implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		String id;
		Buccaneer primaryResource;
		public Chanty(Buccaneer primaryResource) {
			this.id = new Integer(++idGenerator).toString(); 
			this.primaryResource = primaryResource;
		}
		public String getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public String getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		public String getSecondaryIdName()
		{
			return "bucaneer_id";
		}
	}
	protected static class Bottle implements PrimaryAndSecondaryIndexIdentifiable
	{	
		static int current = 0;
		int id;
		Buccaneer primaryResource;
		public Bottle(Buccaneer primaryResource) {
			this.primaryResource = primaryResource;
			id = current++;
		}
		public Integer getIdAsPrimaryIndexInstance() { return id;}
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		public String getSecondaryIdName()
		{
			return "bucaneer_id";
		}
	}
}
