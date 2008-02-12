package org.hivedb.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.hivedb.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Maps;

public class AnnotationHelper {
	public static<T extends Annotation> T getFirstInstanceOfAnnotation(Class entityClass, Class<T> annotationClass) {
		Method m = getFirstMethodWithAnnotation(entityClass, annotationClass);
		return m == null ? null : m.getAnnotation(annotationClass);
	}
	
	public static<T extends Annotation> Method getFirstMethodWithAnnotation(Class entityClass, Class<T> annotationClass) {
		return Atom.getFirstOrNull(getAllMethodsWithAnnotation(entityClass, annotationClass));
	}
	
	public static<T extends Annotation> List<Method> getAllMethodsWithAnnotation(Class entityClass, Class<T> annotationClass) {
		List<Method> methods = Lists.newArrayList();
		for(Method method : entityClass.getMethods()) {
			Method targetMethod = method;
			if( !method.getDeclaringClass().equals(entityClass)) {
				try {
					method.getDeclaringClass().getMethod(method.getName(), method.getParameterTypes());
				} catch (SecurityException e) {
					throw new RuntimeException(e);
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(e);
				}
			}
			if(targetMethod.getAnnotation(annotationClass) != null)
				methods.add(method);
		}
		return methods;
	}
	
	public static Map<Annotation, Object> getAllAnnotatedElements(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for(Annotation a : clazz.getAnnotations())
			elements.put(a, clazz);
		elements.putAll(getAllAnnotatedConstructors(clazz));
		elements.putAll(getAllAnnotatedMethods(clazz));
		elements.putAll(getAllAnnotatedFields(clazz));
		return elements;
	}
	
	public static Map<? extends Annotation, ? extends Object> getAllAnnotatedMethods(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for(Method m : clazz.getMethods())
			for(Annotation a : m.getAnnotations())
				elements.put(a, m);
		return elements;
	}

	public static Map<Annotation, Object> getAllAnnotatedConstructors(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for(Constructor c : clazz.getConstructors())
			for(Annotation a : c.getAnnotations())
				elements.put(a, c);
		return elements;
	}
	
	public static Map<? extends Annotation, ? extends Object> getAllAnnotatedFields(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for(Field f : clazz.getDeclaredFields())
			for(Annotation a : f.getAnnotations())
				elements.put(a, f);
		return elements;
	}
	
	public static<T> T getAnnotationDeeply(Class clazz, String property, Class<? extends T> annotationClass) {
		final Method getter = ReflectionTools.getGetterOfProperty(clazz, property);
		if (getter.getAnnotation((Class)annotationClass) != null)
			return (T)getter.getAnnotation((Class)annotationClass);
		Class<T> owner = ReflectionTools.getOwnerOfMethod(clazz, property);
		if (!owner.equals(clazz))
			return (T)ReflectionTools.getGetterOfProperty(owner, property).getAnnotation((Class)annotationClass);
		return null;
	}
}
