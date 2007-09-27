package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.HiveException;
import org.hivedb.util.GenerateHiveIndexKeys;
import org.hivedb.util.InstallHiveIndexSchema;
import org.hivedb.util.PersisterImpl;
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
	 * @param hiveScenarioConfig - A configuration describing a hive
	 * @param primaryIndexInstanceCount - The number of primary index rows to create
	 * @param resourceInstanceCount - The number of resource rows to create. Resource indexes will be
	 * assigned round-robing to the created primary index rows. 
	 * @return A HiveScenario instance that provides access to the HiveScenarioConfig and instances created
	 * that represent each row inserted in the Hive indexes.
	 * @throws HiveException
	 */
	public static HiveScenario run(HiveScenarioConfig hiveScenarioConfig, int primaryIndexInstanceCount, int resourceInstanceCount) {
		InstallHiveIndexSchema.install(hiveScenarioConfig);
		HiveScenario hiveScenario = new HiveScenario(hiveScenarioConfig, primaryIndexInstanceCount, resourceInstanceCount);
		return hiveScenario;
	}

	private final  HiveScenarioConfig hiveScenarioConfig;
	Collection<Object> primaryIndexKeys;
	Collection<Object> resourceinstances;
	protected HiveScenario(final HiveScenarioConfig hiveScenarioConfig, int primaryIndexInstanceCount, int resourceInstanceCount)
	{
		this.hiveScenarioConfig = hiveScenarioConfig; 
		GenerateHiveIndexKeys generateHiveIndexKeys = new GenerateHiveIndexKeys(
				new PersisterImpl(), 
				primaryIndexInstanceCount,
				resourceInstanceCount);
		 
			
		this.resourceinstances = generateHiveIndexKeys.createResourceInstances(hiveScenarioConfig);
		this.primaryIndexKeys = Filter.getUnique(
			Transform.map(new Unary<Object,Object>() {
				public Object f(Object resourceInstance) {
					return hiveScenarioConfig.getResourceIdentifiable().getPrimaryIndexIdentifiable().getPrimaryIndexKey(resourceInstance);
				
				}},
				resourceinstances));
	}
	
	/** 
	 * @return The PrimaryIndexIdentifiable instances created to correspond to each row inserted in 
	 * the hive's primary index. Use these instances to verify the integrity of the Hive or as representations
	 * of your business objects that are the primary index of the hive.
	 */
	public Collection<Object> getGeneratedPrimaryIndexKeys()
	{
		return primaryIndexKeys;
	}
	/** 
	 * @return The ResourceIdentifiable instances created to correspond to each row inserted in 
	 * the each of the hive's resource index. Use these instances to verify the integrity of the Hive or as represetations
	 * of your business objects that are the resources of the hive.
	 */
	public Collection<Object> getGeneratedResourceInstances() {
		return resourceinstances;
	}

	public HiveScenarioConfig getHiveScenarioConfig() {
		return hiveScenarioConfig;
	}
}

