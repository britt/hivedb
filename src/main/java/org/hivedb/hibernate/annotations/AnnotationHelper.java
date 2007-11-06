package org.hivedb.hibernate.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.hivedb.util.Lists;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Maps;

public class AnnotationHelper {
	public static<T extends Annotation> T getFirstInstanceOfAnnotation(Class entityClass, Class<T> annotationClass) {
		return getFirstMethodWithAnnotation(entityClass, annotationClass).getAnnotation(annotationClass);
	}
	
	public static<T extends Annotation> Method getFirstMethodWithAnnotation(Class entityClass, Class<T> annotationClass) {
		return Atom.getFirstOrThrow(getAllMethodsWithAnnotation(entityClass, annotationClass));
	}
	
	public static<T extends Annotation> List<Method> getAllMethodsWithAnnotation(Class entityClass, Class<T> annotationClass) {
		List<Method> methods = Lists.newArrayList();
		for(Method method : entityClass.getMethods())
			if(method.getAnnotation(annotationClass) != null)
				methods.add(method);
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
}
