package org.hivedb.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveFacade;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Binary;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Ternary;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Filter.BinaryPredicate;

/**
 * HiveUpdater updates a hive to the configuration of a HiveScenarioConfig. When the HiveScenarioConfig's 
 * configuration of its primary index, resources, and secondary indexes differ from that of its Hive
 * instance, it will update the referenced hive to match its configuration. This includes name changes,
 * and the addition or removal of resources and secondary indexes.
 * 
 * @author Andy Likuski
 * TODO rename HiveUpdater
 */
public class HiveSyncer {

	private Hive hive;
	public HiveSyncer(Hive hive)
	{
		this.hive = hive;
	}
	/**
	 *  Sync a Hive to the configuration of the given HiveScenarioConfig. This is an additive process only:
	 *  If the Hive contains Partition Dimensions, Resources, Secondary Indexes, or Data Nodes not
	 *  specified by the HiveScenarioConfig they will be left alone, not deleted.
	 * @param entityHiveConfig
	 * @return
	 * @throws HiveLockableException 
	 */
	public HiveDiff syncHive(EntityHiveConfig entityHiveConfig) throws HiveLockableException
	{
		HiveDiff hiveDiff = diffHive(entityHiveConfig);
		for(Resource resource: hiveDiff.getMissingResources())
			hive.addResource(resource);
		for(Entry<Resource, Collection<SecondaryIndex>> entry : hiveDiff.getMissingSecondaryIndexes().entrySet())
			for(SecondaryIndex index: entry.getValue())
				hive.addSecondaryIndex(entry.getKey(), index);
		return hiveDiff;
	}

	/**
	 *  Returns a HiveDiff the difference between the HiveScenarioConfig instance and the current hive
	 * @param hiveConfig Config file file for the given hive
	 * @return a HiveDiff class that describes what partition dimensions are missing, what resources and nodes are missing
	 * from existing partion dimensions, and what secondary indexes are missing from existing resources
	 */
	public HiveDiff diffHive(final EntityHiveConfig updater)
	{	
		Collection<Resource> missingResources = Lists.newArrayList();
		Map<Resource, Collection<SecondaryIndex>> indexMap = Maps.newHashMap();

		for(EntityConfig config : updater.getEntityConfigs()) {
			try {
				Resource resource = hive.getPartitionDimension().getResource(config.getResourceName());
				for(EntityIndexConfig indexConfig : getHiveIndexes(config)) {
					try {
						resource.getSecondaryIndex(indexConfig.getIndexName());
					} catch(HiveKeyNotFoundException ex) {
						if(!indexMap.containsKey(resource))
							indexMap.put(resource, new ArrayList<SecondaryIndex>());
						indexMap.get(resource).add(configToIndex().f(indexConfig));
					}
				}
			} catch(HiveKeyNotFoundException e) {
				Resource resource = new Resource(
						config.getResourceName(),
						JdbcTypeMapper.primitiveTypeToJdbcType(config.getIdClass()),
						config.isPartitioningResource());
				missingResources.add(resource);
				indexMap.put(resource, Transform.map(configToIndex(), config.getEntityIndexConfigs()));
			}
		}
		return new HiveDiff(missingResources, indexMap);
	}
	private Unary<EntityIndexConfig, SecondaryIndex> configToIndex() {
		return new Unary<EntityIndexConfig, SecondaryIndex>(){
			public SecondaryIndex f(EntityIndexConfig item) {
				return new SecondaryIndex(item.getIndexName(), JdbcTypeMapper.primitiveTypeToJdbcType(item.getIndexClass()));
			}};
	}
	private static Collection<? extends EntityIndexConfig> getHiveIndexes(final EntityConfig entityConfig) {
		return Filter.grep(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return entityIndexConfig.getIndexType().equals(IndexType.Hive);	
			}}, entityConfig.getEntityIndexConfigs());
	}
}
