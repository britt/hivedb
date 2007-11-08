package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityConfigImpl;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

// Creates a miminum EntityConfig instance based on the state of a hive installation.
// This can be used for simple activities like adding a new data node.
public class EntityConfigInducer {

	public static EntityConfig induce(Hive hive, String partitionDimensionName, String resourceName) {
		PartitionDimension partitionDimension = hive.getPartitionDimension();
		Resource resource = partitionDimension.getResource(resourceName);
		
		return resource.isPartitioningResource()
			? EntityConfigImpl.createPartitioningResourceEntity(
				AnonymousEntityInterface.class,
				partitionDimensionName,
				"id",
				createEntitySecondaryIndexes(resource))
			: EntityConfigImpl.createEntity(
				AnonymousEntityInterface.class,
				partitionDimensionName,
				resourceName,
				"primaryIndexKey", 
				"id",
				createEntitySecondaryIndexes(resource));
	}
	
	private static Collection<EntityIndexConfig> createEntitySecondaryIndexes(Resource resource) {
		return Transform.map(new Unary<SecondaryIndex, EntityIndexConfig>() {
			public EntityIndexConfig f(final SecondaryIndex item) {
				return new EntityIndexConfigImpl(
						AnonymousEntityInterface.class, 
						item.getColumnInfo().getName()) 
				{
					public Class<?> getIndexClass() {
						return JdbcTypeMapper.jdbcTypeToPrimitiveClass(item.getColumnInfo().getColumnType());
					}
				};
		}},
		resource.getSecondaryIndexes());
	}
	public interface AnonymousEntityInterface {
		Object getId();
		Object getPrimaryIndexKey();
	}
}
