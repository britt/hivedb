package org.hivedb.util;

import java.util.Collection;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.meta.EntityGenerator;
import org.hivedb.meta.EntityGeneratorImpl;
import org.hivedb.meta.PrimaryIndexKeyGenerator;
import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.RingIteratorable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class GenerateHiveIndexKeys {
	
	private Persister persister = new PersisterImpl();
	private int primaryInstanceCount;
	private int resourceInstanceCount;
	public GenerateHiveIndexKeys(Persister persister, int primaryIndexInstanceCount, int resourceInstanceCount)
	{
		this.persister = persister;
		this.primaryInstanceCount = primaryIndexInstanceCount;
		this.resourceInstanceCount = resourceInstanceCount;
	}
	
	public Collection<Object> createResourceInstances(
		final EntityHiveConfig entityHiveConfig, final Class representedInterface)
	{
		final EntityConfig entityConfig =  entityHiveConfig.getEntityConfig(representedInterface);
		if (entityConfig.isPartitioningResource()
			&& primaryInstanceCount != resourceInstanceCount)
			throw new RuntimeException("For partitioning resources, configure the primaryInstanceCount and resourceInstanceCount equally");
		final Collection<Object> primaryIndexIdentifiables = createPrimaryIndexKeys(entityHiveConfig, representedInterface);
		final EntityGenerator<Object> entityConfigGenerator =
				new EntityGeneratorImpl<Object>(entityConfig);
		
		final Iterable<Object> primaryIndexKeysIterable = new RingIteratorable<Object>(
			primaryIndexIdentifiables,
			resourceInstanceCount);
		
		Collection<Object> resourceInstances = Transform.map(
			new Unary<Object, Object>() {
				public Object f(Object primaryIndexKey) {
					return entityConfigGenerator.generate(primaryIndexKey);
			}},
			primaryIndexKeysIterable);	
		
		for (Object resourceInstance : resourceInstances) {
			// reassign to the result. The returned instance need not be that passed in.
			resourceInstance = persister.persistResourceInstance(entityHiveConfig, representedInterface, resourceInstance);
			Collection<? extends EntityIndexConfig> entitySecondaryIndexConfigs = entityConfig.getEntitySecondaryIndexConfigs();
			for (EntityIndexConfig entitySecondaryIndexConfig : entitySecondaryIndexConfigs) {
				persister.persistSecondaryIndexKey(entityHiveConfig, representedInterface, entitySecondaryIndexConfig, resourceInstance);
			}
		}
		
		return resourceInstances;
	}
	

	/**
	 * @param entityConfig
	 * @return
	 */
	private Collection<Object> createPrimaryIndexKeys(final EntityHiveConfig entityHiveConfig, final Class representedInterface)
	{	
		final Generator primaryIndexIdentifiableGenerator = 
			new PrimaryIndexKeyGenerator(entityHiveConfig.getEntityConfig(representedInterface));
		
		return Generate.create(new Generator<Object>() { 
			public Object generate() {
				try {
					Object primaryIndexKey = primaryIndexIdentifiableGenerator.generate();
					persister.persistPrimaryIndexKey(entityHiveConfig, representedInterface, primaryIndexKey);						
					return primaryIndexKey;
				} catch (Exception e) { throw new RuntimeException(e); }
			}},
			new NumberIterator(primaryInstanceCount));
	}	
}
