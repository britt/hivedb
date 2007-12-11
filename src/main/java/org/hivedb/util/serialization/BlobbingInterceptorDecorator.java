package org.hivedb.util.serialization;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.serialization.Converters;
import org.hivedb.serialization.Serializer;
import org.hivedb.serialization.XmlXStreamSerializer;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;


/**
 * @author andylikuski
 *
 */
public class BlobbingInterceptorDecorator extends InterceptorDecorator implements Interceptor {
	private EntityHiveConfig hiveConfig;
	private final Map<Class, Serializer<Object, InputStream>> serializers;
	
	public BlobbingInterceptorDecorator(EntityHiveConfig hiveConfig) {
		super(EmptyInterceptor.INSTANCE);
		this.hiveConfig = hiveConfig;
		this.serializers = createSerializers(hiveConfig);
	}

	public BlobbingInterceptorDecorator(Interceptor interceptor, EntityHiveConfig hiveConfig) {
		super(interceptor);
		this.hiveConfig = hiveConfig;
		this.serializers = createSerializers(hiveConfig);
	}
	
	private Map<Class, Serializer<Object, InputStream>> createSerializers(EntityHiveConfig hiveConfig) {
		return Transform.toMap(
			new Unary<EntityConfig, Class>() {
				public Class f(EntityConfig entityConfig) {
					return entityConfig.getRepresentedInterface();
			}},
			new Unary<EntityConfig, Serializer<Object,InputStream>>() {
				public Serializer<Object,InputStream> f(EntityConfig entityConfig) {
					return new XmlXStreamSerializer<Object>(entityConfig.getRepresentedInterface());
			}},
			hiveConfig.getEntityConfigs());
	}
	
	private Serializer<Object, InputStream> getSerializer(Object obj) {
		return serializers.get(
			ReflectionTools.whichIsImplemented(
					obj.getClass(),
					serializers.keySet()));
	}
	
	@Override
	public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) throws CallbackException {
		Object serializedEntity =  Converters.getBytes(getSerializer(entity).serialize(entity));
		return super.onSave(serializedEntity, id, state, propertyNames, types);
	}
	
	@Override
	public boolean onFlushDirty(Object entity, Serializable id,
			Object[] currentState, Object[] previousState,
			String[] propertyNames, Type[] types) throws CallbackException {
		
		return super.onFlushDirty(entity, id, currentState, previousState,
				propertyNames, types);
	}

	@Override
	public Object getEntity(String entityName, Serializable id) throws CallbackException {
		final Object object = super.getEntity(entityName, id);
		return getSerializer(object).deserialize((InputStream)object);
	}
}
