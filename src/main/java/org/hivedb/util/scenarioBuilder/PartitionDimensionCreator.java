package org.hivedb.util.scenarioBuilder;

import java.util.Collection;
import java.util.Collections;

import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class PartitionDimensionCreator {
	public static PartitionDimension create(final HiveScenarioConfig hiveScenarioConfig) {
		PrimaryIndexIdentifiable primaryIndexIdentifiable = hiveScenarioConfig.getResourceIdentifiable().getPrimaryIndexIdentifiable();
		String partitionDimensionName = primaryIndexIdentifiable.getPartitionDimensionName();
				
		return new PartitionDimension(
			partitionDimensionName,
			JdbcTypeMapper.primitiveTypeToJdbcType(
					ReflectionTools.getPropertyType(
							hiveScenarioConfig.getResourceIdentifiable().getRepresentedInterface(), 
							primaryIndexIdentifiable.getPrimaryKeyPropertyName())),
			hiveScenarioConfig.getDataNodes(),
			hiveScenarioConfig.getHive().getUri(), // PartitionDimension uri always equals that of the hive
			Collections.singletonList(createResource(hiveScenarioConfig))
		);
	}
	private static Resource createResource(final HiveScenarioConfig hiveScenarioConfig) {
		ResourceIdentifiable<Object> resourceIdentifiable = hiveScenarioConfig.getResourceIdentifiable();
		return new Resource(
				resourceIdentifiable.getResourceName(), 
				JdbcTypeMapper.primitiveTypeToJdbcType(
						ReflectionTools.getPropertyType(
								resourceIdentifiable.getRepresentedInterface(), 
								resourceIdentifiable.getIdPropertyName())),
				resourceIdentifiable.isPartitioningResource(),
				constructSecondaryIndexesOfResource(resourceIdentifiable));
	}
	
	public static Collection<SecondaryIndex> constructSecondaryIndexesOfResource(final ResourceIdentifiable<Object> resourceIdentifiable) {	
		try {
			return 
				Transform.map(
					new Unary<SecondaryIndexIdentifiable, SecondaryIndex>() {
						public SecondaryIndex f(SecondaryIndexIdentifiable secondaryIndexIdentifiable) {
							try {
								return new SecondaryIndex(
										secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName(),
										JdbcTypeMapper.primitiveTypeToJdbcType(
												secondaryIndexIdentifiable.isManyToOneMultiplicity()
													? ReflectionTools.getCollectionItemType(
														resourceIdentifiable.getRepresentedInterface(),
														secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName())
													: ReflectionTools.getPropertyType(
														resourceIdentifiable.getRepresentedInterface(),
														secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName())));				
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
