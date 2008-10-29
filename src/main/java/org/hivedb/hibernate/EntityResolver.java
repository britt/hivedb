package org.hivedb.hibernate;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.IndexDelegate;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.entity.EntityConfig;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.configuration.entity.EntityIndexConfig;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.GeneratedImplementation;
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
	
	// The given class may or may not be a Hive entity class. If it is return it's interface, otherwise
	// search throught the EntityHiveConfigs for an IndexDelegate whose type matches the type given.
	// This method grabs to first match, so it fill be inaccurate if two EntityHiveConfigs share a delegated
	// property type, or if two methods within And EntityConfig have the same delegated property type
	public Class<?> resolveToEntityOrRelatedEntity(Class<?> clazz) {
		Class<?> entityInterface = resolveEntityInterface(clazz);
		if (entityInterface != null) 
			return  entityInterface; 
		try {
			for (EntityConfig entityConfig : entityHiveConfig.getEntityConfigs()) {
				for (EntityIndexConfig entityIndexConfig : entityConfig.getEntityIndexConfigs()) {
					if (entityIndexConfig.getIndexType().equals(IndexType.Delegates)) {
						final String propertyName = entityIndexConfig.getPropertyName();
						final Class<?> representedInterface = entityConfig.getRepresentedInterface();
						if (ReflectionTools.isCollectionProperty(representedInterface, propertyName)) {
							if (ReflectionTools.doesImplementOrExtend(
									clazz,
									ReflectionTools.getCollectionItemType(representedInterface, propertyName))) {
								Method method = ReflectionTools.getGetterOfProperty(representedInterface, propertyName);
								return Class.forName(method.getAnnotation(IndexDelegate.class).value());
							}
						}
						else
							if (ReflectionTools.doesImplementOrExtend(
									clazz,
									ReflectionTools.getPropertyType(representedInterface, propertyName))) {
								Method method = ReflectionTools.getGetterOfProperty(representedInterface, propertyName);
								return Class.forName(method.getAnnotation(IndexDelegate.class).value());
							}			
					}
				}
			}
			return null;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Class not found " + getIndexDelegateAnnotatedMethod(clazz).getAnnotation(IndexDelegate.class).value());
		}
	}
	@SuppressWarnings("unchecked")
	public Map.Entry<Class<?>, Object>  resolveToEntityOrRelatedEntiyInterfaceAndId(Object entity) {
		Class clazz = entity.getClass();
		Class entityInterface = resolveEntityInterface(clazz);
		if (entityInterface != null) 
			return  new Pair<Class<?>, Object>(entityInterface,entityHiveConfig.getEntityConfig(entityInterface).getId(entity));
		Method method = getIndexDelegateAnnotatedMethod(clazz);
		try {
			
			return  new Pair<Class<?>, Object>(
					Class.forName(method.getAnnotation(IndexDelegate.class).value()),
					method.invoke(entity, new Object[]{}));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	@SuppressWarnings("unchecked")
	public Class resolveEntityInterface(Class clazz) {
		return ReflectionTools.whichIsImplemented(
				clazz, 
				Transform.map(new Unary<EntityConfig, Class>() {
					public Class f(EntityConfig entityConfig) {
						return entityConfig.getRepresentedInterface();
					}},
					entityHiveConfig.getEntityConfigs()));
	}
	
	// Given a class that is not an entity itself, see it is the type of
	// of a delegated index of an entity
	@SuppressWarnings("unchecked")
	private Method getIndexDelegateAnnotatedMethod(Class clazz) {
		Method annotatedMethod = Filter.grepSingleOrNull(new Predicate<Method>() {
			public boolean f(Method method) {
				return method.getAnnotation(IndexDelegate.class) != null;
		}}, Transform.flatMap(new Unary<Class, Collection<Method>>() {
			public Collection<Method> f(Class interfase) {
				return Arrays.asList(interfase.getMethods());
			}}, Arrays.asList(clazz.getInterfaces())));
		return annotatedMethod;
	}
	public static Class<?> getPersistedImplementation(Class<?> clazz) {
		if(generatesImplementation(clazz))
			return GeneratedClassFactory.getGeneratedClass(clazz);
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
