package org.hivedb.util;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.NodeGroup;
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
			final Hive hive) throws HiveException {
		
		// Create a partition dimension and its subordinate NodeGroup, primary Node, Resources, and SecondaryIndexes
		PartitionDimension partitionDimension;
		try {
			partitionDimension = createPartitionDimension(hiveScenarioConfig);
			hive.addPartitionDimension(partitionDimension);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
				
		try {
			hive.create();
		}
		catch (Exception exception)
		{
			throw new RuntimeException(exception);
		}
		return partitionDimension;
	}

	public static PartitionDimension createPartitionDimension(final HiveScenarioConfig hiveScenarioConfig) {
		final NodeGroup nodeGroup = new NodeGroup(hiveScenarioConfig.getDataNodes());
		
		final Collection<Resource> resources = 
			Transform.map(new Unary<ResourceIdentifiable, Resource>() {
				public Resource f(ResourceIdentifiable resourceIdentifiable) { 
					return new Resource(resourceIdentifiable.getResourceName(), constructSecondaryIndexesOfResource(resourceIdentifiable));
				}},
				hiveScenarioConfig.getPrimaryIndexIdentifiable().getResourceIdentifiables());
		PartitionDimension partitionDimension = new PartitionDimension(
				hiveScenarioConfig.getPrimaryIndexIdentifiable().getPartitionDimensionName(),
			JdbcTypeMapper.primitiveTypeToJdbcType(hiveScenarioConfig.getPrimaryIndexIdentifiable().getPrimaryIndexKey().getClass()),
			nodeGroup,
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
						public SecondaryIndex f(SecondaryIndexIdentifiable secondaryIndexIdentifiable) {
							try {
								return new SecondaryIndex(
										new ColumnInfo(
											secondaryIndexIdentifiable.getSecondaryIndexColumnName(),											
											JdbcTypeMapper.primitiveTypeToJdbcType(secondaryIndexIdentifiable.getSecondaryIndexKey().getClass())));
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
