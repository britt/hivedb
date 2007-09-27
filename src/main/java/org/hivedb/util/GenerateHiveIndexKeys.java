package org.hivedb.util;

import java.util.Collection;

import org.hivedb.meta.PrimaryIndexIdentifiableGeneratableImpl;
import org.hivedb.meta.ResourceIdentifiableGeneratable;
import org.hivedb.meta.ResourceIdentifiableGeneratableImpl;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.RingIteratorable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;

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
		final HiveScenarioConfig hiveScenarioConfig)
	{
		final Collection<Object> primaryIndexIdentifiables = createPrimaryIndexKeys(hiveScenarioConfig);
		final ResourceIdentifiableGeneratable<Object> resourceIdentifiableGenerator =
				new ResourceIdentifiableGeneratableImpl<Object>(hiveScenarioConfig.getResourceIdentifiable());
		
		final Iterable<Object> primaryIndexKeysIterable = new RingIteratorable<Object>(
			primaryIndexIdentifiables,
			resourceInstanceCount);
		
		Collection<Object> resourceInstances = Transform.map(
			new Unary<Object, Object>() {
				public Object f(Object primaryIndexKey) {
					return resourceIdentifiableGenerator.generate(primaryIndexKey);
			}},
			primaryIndexKeysIterable);	
		
		for (Object resourceInstance : resourceInstances) {
			// reassign to the result. The returned instance need not be that passed in.
			resourceInstance = persister.persistResourceInstance(hiveScenarioConfig, resourceInstance);
			Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables = hiveScenarioConfig.getResourceIdentifiable().getSecondaryIndexIdentifiables();
			for (SecondaryIndexIdentifiable secondaryIndexIdentifiable : secondaryIndexIdentifiables) {
				persister.persistSecondaryIndexKey(hiveScenarioConfig, secondaryIndexIdentifiable, resourceInstance);
			}
		}
		
		return resourceInstances;
	}
	

	/**
	 * @param hiveScenarioConfig
	 * @return
	 */
	private Collection<Object> createPrimaryIndexKeys(final HiveScenarioConfig hiveScenarioConfig)
	{	
		final Generator primaryIndexIdentifiableGenerator = 
			new PrimaryIndexIdentifiableGeneratableImpl(hiveScenarioConfig.getResourceIdentifiable());
		
		return Generate.create(new Generator<Object>() { 
			public Object f() {
				try {
					Object primaryIndexKey = primaryIndexIdentifiableGenerator.f();
					persister.persistPrimaryIndexKey(hiveScenarioConfig, primaryIndexKey);						
					return primaryIndexKey;
				} catch (Exception e) { throw new RuntimeException(e); }
			}},
			new NumberIterator(primaryInstanceCount));
	}	
}
