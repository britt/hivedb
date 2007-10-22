package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.HiveException;
import org.hivedb.meta.HiveConfig;
import org.hivedb.util.GenerateHiveIndexKeys;
import org.hivedb.util.InstallHiveIndexSchema;
import org.hivedb.util.Persister;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

/**
 *
 * @author andylikuski Andy Likuski
 * 
 *  Creates a hive from the given HiveScenarioConfig and dataNodes, then generates primary index, resource,
 *  and secondary index rows in the created hive based on the HiveScenarioConfig and the specified number
 *  to create. Each row is represented by a PrimaryIndexIdentifiable, ResourceIdentifiable, or SecondaryIndexIdentifiable
 *  instance, respectively. These instances are available to verify that the contents of the hive match
 *  the instances. 
 */
public class HiveScenario {
	
	/**
	 *  Create a hive based on the give HiveScenarioConfig and dataNodes. Populate the primary index, resource,
	 *  and secondary index rows with generated values.
	 * @param hiveConfig - A configuration describing a hive
	 * @param primaryIndexInstanceCount - The number of primary index rows to create
	 * @param resourceInstanceCount - The number of resource rows to create. Resource indexes will be
	 * assigned round-robing to the created primary index rows. 
	 * @return A HiveScenario instance that provides access to the HiveScenarioConfig and instances created
	 * that represent each row inserted in the Hive indexes.
	 * @throws HiveException
	 */
	public static HiveScenario run(HiveConfig hiveConfig, int primaryIndexInstanceCount, int resourceInstanceCount, Persister persister) {
		InstallHiveIndexSchema.install(hiveConfig);
		HiveScenario hiveScenario = new HiveScenario(hiveConfig, primaryIndexInstanceCount, resourceInstanceCount, persister);
		return hiveScenario;
	}

	private final  HiveConfig hiveConfig;
	Collection<Object> primaryIndexKeys;
	Collection<Object> resourceinstances;
	protected HiveScenario(final HiveConfig hiveConfig, int primaryIndexInstanceCount, int resourceInstanceCount, final Persister persister)
	{
		this.hiveConfig = hiveConfig; 
		GenerateHiveIndexKeys generateHiveIndexKeys = new GenerateHiveIndexKeys(
				persister, 
				primaryIndexInstanceCount,
				resourceInstanceCount);
		
		this.resourceinstances = generateHiveIndexKeys.createResourceInstances(hiveConfig);
		this.primaryIndexKeys = Filter.getUnique(
			Transform.map(new Unary<Object,Object>() {
				public Object f(Object resourceInstance) {
					return hiveConfig.getEntityConfig().getPrimaryIndexKey(resourceInstance);
				}},
				resourceinstances));
	}

	public Collection<Object> getGeneratedPrimaryIndexKeys()
	{
		return primaryIndexKeys;
	}

	public Collection<Object> getGeneratedResourceInstances() {
		return resourceinstances;
	}

	public HiveConfig getHiveConfig() {
		return hiveConfig;
	}
}

