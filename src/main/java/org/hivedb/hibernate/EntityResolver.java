package org.hivedb.hibernate;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.IndexDelegate;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GeneratedImplementation;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class EntityResolver {
	
	private EntityHiveConfig entityHiveConfig;
	public EntityResolver(EntityHiveConfig entityHiveConfig) {
		this.entityHiveConfig = entityHiveConfig; 
	}
	
	public Collection<Class<?>> getEntityClasses() {
		return Transform.map(new Unary<EntityConfig, Class<?>>() {
			public Class<?> f(EntityConfig entityConfig) {
				return entityConfig.getRepresentedInterface();
			}}, 
		this.entityHiveConfig.getEntityConfigs());
	}
	
	public Class<?> resolveToEntityOrRelatedEntity(Class<?> clazz) {
		Class<?> entityInterface = resolveEntityInterface(clazz);
		if (entityInterface != null) 
			return  entityInterface;
		return getHiveForeignKeyAnnotatedMethod(clazz).getAnnotation(IndexDelegate.class).value();
	}
	@SuppressWarnings("unchecked")
	public Map.Entry<Class<?>, Object>  resolveToEntityOrRelatedEntiyInterfaceAndId(Object entity) {
		Class clazz = entity.getClass();
		Class entityInterface = resolveEntityInterface(clazz);
		if (entityInterface != null) 
			return  new Pair<Class<?>, Object>(entityInterface,entityHiveConfig.getEntityConfig(entityInterface).getId(entity));
		Method method = getHiveForeignKeyAnnotatedMethod(clazz);
		try {
			return  new Pair<Class<?>, Object>(
					method.getAnnotation(IndexDelegate.class).value(),
					method.invoke(entity, new Object[]{}));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public Class resolveEntityInterface(Class clazz) {
		return ReflectionTools.whichIsImplemented(
				clazz, 
				Transform.map(new Unary<EntityConfig, Class>() {
					public Class f(EntityConfig entityConfig) {
						return entityConfig.getRepresentedInterface();
					}},
					entityHiveConfig.getEntityConfigs()));
	}
	private Method getHiveForeignKeyAnnotatedMethod(Class clazz) {
		Method annotatedMethod = Filter.grepSingleOrNull(new Predicate<Method>() {
			public boolean f(Method method) {
				return method.getAnnotation(IndexDelegate.class) != null;
		}}, Transform.flatMap(new Unary<Class, Collection<Method>>() {
			public Collection<Method> f(Class interfase) {
				return Arrays.asList(interfase.getMethods());
			}}, Arrays.asList(clazz.getInterfaces())));
		if (annotatedMethod == null)
			throw new RuntimeException(
				String.format("Class %s cannot be resolved to a Hive enity and does not reference a Hive entity", clazz.getCanonicalName()));
		return annotatedMethod;
	}
	public static Class<?> getPersistedImplementation(Class<?> clazz) {
		if(generatesImplementation(clazz))
			return GeneratedInstanceInterceptor.getGeneratedClass(clazz);
		else
			return clazz;
	}
	
	public static boolean generatesImplementation(Class<?> clazz) {
		return clazz.getAnnotation(GeneratedClass.class) != null;
	}

	public static boolean isGeneratedImplementation(Class<?> clazz) {
		return Arrays.asList(clazz.getInterfaces()).contains(GeneratedImplementation.class);
	}

}
