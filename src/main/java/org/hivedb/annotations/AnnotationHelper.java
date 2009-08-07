package org.hivedb.annotations;

import org.hivedb.util.Lists;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Predicate;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class AnnotationHelper {

	// stores whether Annotation exists for given Method/AnnotationClass (cache to avoid reflection)
	private static final MethodClassMappings<Boolean> hasAnnotationByAnnotationClass = new MethodClassMappings<Boolean>();

	// stores Annotation instance for given Method/AnnotationClass (cache to avoid reflection)
	private static final MethodClassMappings<Annotation> deepAnnotationByAnnotationClass = new MethodClassMappings<Annotation>();

	@SuppressWarnings("unchecked")
	public static <T extends Annotation> T getFirstInstanceOfAnnotation(Class entityClass, Class<T> annotationClass) {
		Method m = getFirstMethodWithAnnotation(entityClass, annotationClass);
		return m == null ? null : m.getAnnotation(annotationClass);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Annotation> Method getFirstMethodWithAnnotation(Class entityClass, Class<T> annotationClass) {
		return Atom.getFirstOrNull(getAllMethodsWithAnnotation(entityClass, annotationClass));
	}

	@SuppressWarnings("unchecked")
	public static List<Method> getAllMethodsWithAnnotation(Class entityClass, Class annotationClass) {
		return getAllMethodsWithAnnotations(entityClass, Collections.singleton(annotationClass));
	}

	@SuppressWarnings("unchecked")
	public static List<Method> getAllMethodsWithAnnotations(Class entityClass, Collection<Class> annotationClasses) {
		List<Method> methods = Lists.newArrayList();
		for (Method method : entityClass.getMethods()) {
			if (!method.getDeclaringClass().equals(entityClass)) {
				try {
					method.getDeclaringClass().getMethod(method.getName(), method.getParameterTypes());
				} catch (SecurityException e) {
					throw new RuntimeException(e);
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(e);
				}
			}

			try {
				// method.getAnnotation(annotationClass) is expensive so we use caching
				for (Class annotationClass : annotationClasses) {
					if (hasAnnotationByAnnotationClass.contains(method, annotationClass)) {
						if (hasAnnotationByAnnotationClass.get(method, annotationClass)) {
							methods.add(method);
							break;
						}
					} else {
						if (method.getAnnotation(annotationClass) != null) {
							methods.add(method);
							hasAnnotationByAnnotationClass.add(method, annotationClass, Boolean.TRUE);
							break;
						} else {
							hasAnnotationByAnnotationClass.add(method, annotationClass, Boolean.FALSE);
						}
					}
				}
			} catch (RuntimeException e) {
				System.out.println(e.getMessage());
				throw e;
			}

		}

		return methods;
	}

	@SuppressWarnings("unchecked")
	public static Map<Annotation, Object> getAllAnnotatedElements(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for (Annotation a : clazz.getAnnotations())
			elements.put(a, clazz);
		elements.putAll(getAllAnnotatedConstructors(clazz));
		elements.putAll(getAllAnnotatedMethods(clazz));
		elements.putAll(getAllAnnotatedFields(clazz));
		return elements;
	}

	@SuppressWarnings("unchecked")
	public static Map<? extends Annotation, ? extends Object> getAllAnnotatedMethods(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for (Method m : clazz.getMethods())
			for (Annotation a : m.getAnnotations())
				elements.put(a, m);
		return elements;
	}

	@SuppressWarnings("unchecked")
	public static Map<Annotation, Object> getAllAnnotatedConstructors(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for (Constructor c : clazz.getConstructors())
			for (Annotation a : c.getAnnotations())
				elements.put(a, c);
		return elements;
	}

	@SuppressWarnings("unchecked")
	public static Map<? extends Annotation, ? extends Object> getAllAnnotatedFields(Class clazz) {
		Map<Annotation, Object> elements = Maps.newHashMap();
		for (Field f : clazz.getDeclaredFields())
			for (Annotation a : f.getAnnotations())
				elements.put(a, f);
		return elements;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getAnnotationDeeply(Class clazz, String property, Class<? extends T> annotationClass) {
		return getAnnotationDeeply(ReflectionTools.getGetterOfProperty(clazz, property), annotationClass);
	}

	@SuppressWarnings("unchecked")
	public static <T> T getAnnotationDeeply(Method method, Class<? extends T> annotationClass) {
		if (deepAnnotationByAnnotationClass.contains(method,annotationClass)) {
			return (T) deepAnnotationByAnnotationClass.get(method, annotationClass);
		}

		Annotation annotation = method.getAnnotation((Class) annotationClass);
		if (annotation == null) {
			final Method ownerMethod = ReflectionTools.getMethodOfOwner(method);
			Class<T> owner = (Class<T>) ownerMethod.getDeclaringClass();
			if (!owner.equals(method.getDeclaringClass())) {
				annotation = ownerMethod.getAnnotation((Class) annotationClass);
			}
		}

		deepAnnotationByAnnotationClass.add(method,annotationClass, annotation);
		return (T) annotation;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getMethodArgumentAnnotationDeeply(Method method, int argumentIndex, final Class<? extends T> annotationClass) {
		return (T) Filter.grepSingleOrNull(
				new Predicate<Annotation>() {
					public boolean f(Annotation annoation) {
						return ReflectionTools.doesImplementOrExtend(annoation.getClass(), annotationClass);
					}
				},
				Arrays.asList(method.getParameterAnnotations()[argumentIndex]));

	}

	/**
	 * Associates Method-Class mappings with values
	 */
	private static class MethodClassMappings<V> {

		private final Map<Method, Map<Class, V>> cache = Maps.newHashMap();

		public boolean contains(Method m, Class obj) {
			Map<Class, V> methodMappings = cache.get(m);
			return methodMappings != null && methodMappings.containsKey(obj);
		}

		public V get(Method m, Class obj) {
			Map<Class, V> methodMappings = cache.get(m);
			if (methodMappings == null) {
				throw new RuntimeException("No data for specified method/class");
			}
			return methodMappings.get(obj);
		}

		public void add(Method m, Class obj, V value) {
			Map<Class, V> methodMappings = cache.get(m);
			if (methodMappings == null) {
				methodMappings = Maps.newHashMap();
				cache.put(m, methodMappings);
			}

			methodMappings.put(obj, value);

			System.out.println("cache: " + cache.size() + " thisMethod: " + methodMappings.size());
		}

	}
}
