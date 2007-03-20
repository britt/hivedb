package org.hivedb.util;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.HiveException;
import org.hivedb.meta.Hive;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.scenarioBuilder.Generate;
import org.hivedb.util.scenarioBuilder.Generator;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;
import org.hivedb.util.scenarioBuilder.NumberIterator;
import org.hivedb.util.scenarioBuilder.PrimaryAndSecondaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.PrimaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.RingIteratorable;
import org.hivedb.util.scenarioBuilder.SecondaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.Transform;
import org.hivedb.util.scenarioBuilder.Unary;

public class GenerateHiveIndexKeys {
	public Map<Class, Collection<PrimaryIndexIdentifiable>> createPrimaryIndexInstances(final Hive hive, Map<Class, PartitionDimension> partitionDimensionMap, final int countPerPrimaryIndex)
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
	
	public Map<Class, Map<Resource, Map<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>>> createSecondaryIndexInstances(
		final HiveScenarioConfig hiveScenarioConfig,
		final Hive hive,
		final Map<Class, PartitionDimension> partitionDimensionMap,
		final Map<Class, Collection<PrimaryIndexIdentifiable>> primaryIndexInstanceMap)
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
								final Class secondaryIndexClass = hiveScenarioConfig.getResourceNameToClassMap().get(resource.getName());
								
								return
								Transform.toMap(
									new Transform.IdentityFunction<SecondaryIndex>(),
									new Unary<SecondaryIndex, Collection<SecondaryIndexIdentifiable>>() {				
										public Collection<SecondaryIndexIdentifiable> f(final SecondaryIndex secondaryIndex) {
											final boolean sameClass = (secondaryIndexClass.equals(primaryPartitionClass));
											Map<String, Collection<PrimaryIndexIdentifiable>> resourceNameToPrimaryIndexInstanceMap = Transform.connectMaps(Transform.connectMaps(hiveScenarioConfig.getResourceNameToClassMap(), hiveScenarioConfig.getResourceToPrimaryMap()), primaryIndexInstanceMap);
											Collection<PrimaryIndexIdentifiable> primaryInstances = resourceNameToPrimaryIndexInstanceMap.get(InstallHiveIndexSchema.getResourceIdentifiablePrototype(primaryPartitionClass, secondaryIndexClass).getResourceName());
											// Make a RingIterable that iterates over the primary instancees,
											// for countPerSecondaryIndex, but limit it it to the number of primaryInstances to prevent duplicate primary keys
											// when the secondaryInstance is the same as the primaryInstance
											final Iterable<PrimaryIndexIdentifiable> primaryPartitionIndexInstanceIterable = new RingIteratorable<PrimaryIndexIdentifiable>(
												primaryInstances,
												sameClass
													? Math.min(hiveScenarioConfig.getInstanceCountPerSecondaryIndex(), primaryInstances.size())
													: hiveScenarioConfig.getInstanceCountPerSecondaryIndex());
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
