package org.hivedb.util;

import java.util.Collection;

import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.PrimaryIndexKeyGenerator;
import org.hivedb.meta.EntityGenerator;
import org.hivedb.meta.EntityGeneratorImpl;
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
		final SingularHiveConfig hiveConfig)
	{
		final Collection<Object> primaryIndexIdentifiables = createPrimaryIndexKeys(hiveConfig);
		final EntityGenerator<Object> entityConfigGenerator =
				new EntityGeneratorImpl<Object>(hiveConfig.getEntityConfig());
		
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
			resourceInstance = persister.persistResourceInstance(hiveConfig, resourceInstance);
			Collection<? extends EntityIndexConfig> entitySecondaryIndexConfigs = hiveConfig.getEntityConfig().getEntitySecondaryIndexConfigs();
			for (EntityIndexConfig entitySecondaryIndexConfig : entitySecondaryIndexConfigs) {
				persister.persistSecondaryIndexKey(hiveConfig, entitySecondaryIndexConfig, resourceInstance);
			}
		}
		
		return resourceInstances;
	}
	

	/**
	 * @param hiveConfig
	 * @return
	 */
	private Collection<Object> createPrimaryIndexKeys(final SingularHiveConfig hiveConfig)
	{	
		final Generator primaryIndexIdentifiableGenerator = 
			new PrimaryIndexKeyGenerator(hiveConfig.getEntityConfig());
		
		return Generate.create(new Generator<Object>() { 
			public Object generate() {
				try {
					Object primaryIndexKey = primaryIndexIdentifiableGenerator.generate();
					persister.persistPrimaryIndexKey(hiveConfig, primaryIndexKey);						
					return primaryIndexKey;
				} catch (Exception e) { throw new RuntimeException(e); }
			}},
			new NumberIterator(primaryInstanceCount));
	}	
}
