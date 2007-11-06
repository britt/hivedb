package org.hivedb.meta;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
			cloneDataNodes(hiveConfig.getDataNodes()), // clone because nodes are are given the new partion dimension id
			hiveConfig.getHive().getUri(), // PartitionDimension uri always equals that of the hive
			cloneResources(Collections.singletonList(createResource(hiveConfig))) // clone because resources are given the new partition dimension id
		);
		dimension.updateId(hiveConfig.getHive().getPartitionDimension().getId());
		return dimension;
	}
	
	private static Collection<Resource> cloneResources(List<Resource> resources) {
		return Transform.map(new Unary<Resource,Resource>() {
			public Resource f(Resource resource) {
				return new Resource(resource.getId(), resource.getName(), resource.getColumnType(), resource.isPartitioningResource(), resource.getSecondaryIndexes());
			}
		}, resources);
	}

	private static Collection<Node> cloneDataNodes(Collection<Node> dataNodes) {
		return Transform.map(new Unary<Node,Node>() {
			public Node f(Node node) {
				return new Node(node.getId(), node.getName(), node.getDatabaseName(), node.getHost(), node.getPartitionDimensionId(), node.getDialect());
			}
		}, dataNodes);
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
