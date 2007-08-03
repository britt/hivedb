package org.hivedb.util;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;


public class InstallHiveIndexSchema {
	public static PartitionDimension install(
			final HiveScenarioConfig hiveScenarioConfig,
			final Hive hive) {
		
		final PartitionDimension partitionDimension = createPartitionDimension(hiveScenarioConfig);	
		try {
			// Create or update a partition dimension and its subordinate NodeGroup, primary Node, Resources, and SecondaryIndexes
			new HiveSyncer(hive).syncHive(hiveScenarioConfig);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		// Create any missing tables of primary and secondary indexes
		try {

			for (PartitionDimension pd : hive.getPartitionDimensions())
				new IndexSchema(pd).install();
		}
		catch (Exception exception)
		{
			throw new RuntimeException(exception);
		}
		return partitionDimension;
	}

	public static PartitionDimension createPartitionDimension(final HiveScenarioConfig hiveScenarioConfig) {
		final Collection<Resource> resources = 
			Transform.map(new Unary<ResourceIdentifiable, Resource>() {
				public Resource f(ResourceIdentifiable resourceIdentifiable) { 
					return new Resource(
							resourceIdentifiable.getResourceName(), 
							JdbcTypeMapper.primitiveTypeToJdbcType(resourceIdentifiable.getId().getClass()), 
							constructSecondaryIndexesOfResource(resourceIdentifiable));
				}},
				hiveScenarioConfig.getPrimaryIndexIdentifiable().getResourceIdentifiables());
		PartitionDimension partitionDimension = new PartitionDimension(
				hiveScenarioConfig.getPrimaryIndexIdentifiable().getPartitionDimensionName(),
			JdbcTypeMapper.primitiveTypeToJdbcType(hiveScenarioConfig.getPrimaryIndexIdentifiable().getPrimaryIndexKey().getClass()),
			hiveScenarioConfig.getDataNodes(),
			hiveScenarioConfig.getHiveIndexesUri(),
			resources
		);
		return partitionDimension;
	}
	
	public static Collection<SecondaryIndex> constructSecondaryIndexesOfResource(final ResourceIdentifiable resourceIdentifiable) {	
		try {
			return 
				Transform.map(
					new Unary<SecondaryIndexIdentifiable, SecondaryIndex>() {
						public SecondaryIndex f(SecondaryIndexIdentifiable secondaryIndexIdentifiablePrototype) {
							try {
								return new SecondaryIndex(
										secondaryIndexIdentifiablePrototype.getSecondaryIndexColumnName(),
										JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexIdentifiablePrototype.getSecondaryIndexKey().getClass()));
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
					}}, 
					resourceIdentifiable.getSecondaryIndexIdentifiables());
					
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
}
