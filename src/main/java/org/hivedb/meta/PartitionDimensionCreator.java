package org.hivedb.meta;

import java.util.Collection;
import java.util.Collections;

import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class PartitionDimensionCreator {
	public static PartitionDimension create(final HiveConfig hiveConfig) {
		EntityConfig entityConfig = hiveConfig.getEntityConfig();
		String partitionDimensionName = entityConfig.getPartitionDimensionName();
				
		PartitionDimension dimension = new PartitionDimension(
			partitionDimensionName,
			JdbcTypeMapper.primitiveTypeToJdbcType(
					ReflectionTools.getPropertyType(
							hiveConfig.getEntityConfig().getRepresentedInterface(), 
							entityConfig.getPrimaryIndexKeyPropertyName())),
			hiveConfig.getDataNodes(),
			hiveConfig.getHive().getUri(), // PartitionDimension uri always equals that of the hive
			Collections.singletonList(createResource(hiveConfig))
		);
		dimension.updateId(1);
		return dimension;
	}
	private static Resource createResource(final HiveConfig hiveConfig) {
		EntityConfig<?> entityConfig = hiveConfig.getEntityConfig();
		Resource resource = new Resource(
				entityConfig.getResourceName(), 
				JdbcTypeMapper.primitiveTypeToJdbcType(
						ReflectionTools.getPropertyType(
								entityConfig.getRepresentedInterface(), 
								entityConfig.getIdPropertyName())),
				entityConfig.isPartitioningResource(),
				constructSecondaryIndexesOfResource(entityConfig));
		resource.updateId(1);
		return resource;
	}
	
	public static Collection<SecondaryIndex> constructSecondaryIndexesOfResource(final EntityConfig<?> entityConfig) {	
		try {
			return 
				Transform.map(
					new Unary<EntityIndexConfig, SecondaryIndex>() {
						public SecondaryIndex f(EntityIndexConfig secondaryIndexIdentifiable) {
							try {
								return new SecondaryIndex(
									secondaryIndexIdentifiable.getIndexName(),
									JdbcTypeMapper.primitiveTypeToJdbcType(
										secondaryIndexIdentifiable.getIndexClass()));											
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
					}}, 
					entityConfig.getEntitySecondaryIndexConfigs());
					
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
}
