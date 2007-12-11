package org.hivedb.util.serialization;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.hibernate.Session;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.DelegateDataAccessObject;
import org.hivedb.serialization.Converters;
import org.hivedb.serialization.Serializer;
import org.hivedb.serialization.XmlXStreamSerializer;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class BlobbingDelegateDataAccessObject implements
		DelegateDataAccessObject<Object, Serializable> {

	private Class<?> clazz;
	private EntityHiveConfig config;
	
	private Serializer<Object, InputStream> serializer;

	public BlobbingDelegateDataAccessObject(Class<?> clazz, EntityHiveConfig config, Hive hive) {
		this.clazz = clazz;
		this.config = config;
		this.serializer = createSerializer(config);
	}

	public Serializable delete(Serializable id, Session session,
			Object deletedEntity) {
		BlobbedEntity blobbedEntity = createBlobbedEntity(id, deletedEntity);
		session.delete(blobbedEntity);
		return id;
	}

	private BlobbedEntity createBlobbedEntity(Serializable id, Object entity) {
		BlobbedEntity blobbedEntity = GeneratedInstanceInterceptor.newInstance(BlobbedEntity.class);
		blobbedEntity.setId((Integer)id);
		blobbedEntity.setValue(Converters.getBytes(getSerializer().serialize(entity)));
		return blobbedEntity;
	}

	public Collection<Object> findByProperty(String propertyName,
			Object propertyValue, final Session session,
			Collection<Object> partialEntities) {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		return Transform.map(new Unary<Object, Object>() {
			public Object f(Object partialEntity) {
				return deserialize((BlobbedEntity)session.get(getBlobbedEntityClass(),
						entityConfig.getId(partialEntity)));
			}
		}, partialEntities);
	}

	public Object get(Serializable id, Session session, Object partialEntity) {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		return createBlobbedEntity(entityConfig.getId(partialEntity),
				partialEntity);
	}

	public Object save(Object fullEntity, Session session) {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		session.saveOrUpdate(createBlobbedEntity(entityConfig.getId(fullEntity), fullEntity));
		return fullEntity;
	}

	public Collection<Object> saveAll(Collection<Object> fullEntities,
			Session session) {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		for (Object fullEntity : fullEntities)
			session.saveOrUpdate(createBlobbedEntity(entityConfig
					.getId(fullEntity), fullEntity));
		return fullEntities;
	}

	private Serializer<Object, InputStream> getSerializer() {
		return serializer;
	}
	private Serializer<Object, InputStream> createSerializer(EntityHiveConfig hiveConfig) {
		return new XmlXStreamSerializer<Object>(resolveEntityClass(clazz));
	}
	private Class resolveEntityClass(Class clazz) {
		return ReflectionTools.whichIsImplemented(
				clazz, 
				Transform.map(new Unary<EntityConfig, Class>() {
					public Class f(EntityConfig entityConfig) {
						return entityConfig.getRepresentedInterface();
					}},
					config.getEntityConfigs()));
	}

	private Object deserialize(BlobbedEntity blobbedEntity) {
		return getSerializer().deserialize(new ByteArrayInputStream(blobbedEntity.getValue()));
	}

	private Class getBlobbedEntityClass() {
		return GeneratedInstanceInterceptor.getGeneratedClass(BlobbedEntity.class);
	}

}
