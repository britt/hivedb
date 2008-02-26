package org.hivedb.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Joiner.ConcatStrings;
import org.springframework.beans.BeanUtils;

public class ReflectionTools {
	
	public static final class Descriptor {
		private final Class<?> clazz;
		private final Collection<Method> deepMethods;
		private final Collection<Method> declaredPublicMethods;
		private final Map<Method, Method> accessors;
		
		private final Map<Method, String> propertyByGetter;
		private final Map<String, Method> getterByProperty;
		
		private final Map<String, Class<?>> ownerByProperty;
		
		private final Map<Method, Map<Class<?>, Method>> settersByGetter;
		
		private final Map<Method, String> propertyByAccessor;
		
		private Method rawSetter;
		
		private interface PropertySetterWrapper {
			void set(Object instance, Object value);
		}
		
		Descriptor(Class<?> clazz) {
			this.clazz = clazz;
			
			deepMethods = Collections.unmodifiableCollection(ReflectionTools.getDeepMethods(clazz));
			
			declaredPublicMethods = new HashSet<Method>();
			for (Method method : clazz.getDeclaredMethods()) {
				if ((method.getModifiers() & Modifier.PUBLIC) == Modifier.PUBLIC) {
					declaredPublicMethods.add(method);
				}
			}
			
			accessors = new HashMap<Method, Method>();
			for (Method method : clazz.getMethods()) {
				if (ReflectionTools.isGetter(method)) {
					Method setter = BeanUtils.findPropertyForMethod(method).getWriteMethod();
					if (setter != null)
						accessors.put(method, setter);
					else {
						// Interface has no setter, create a wrapper setter
						final String propertyName = BeanUtils.findPropertyForMethod(method).getName(); // bypass our caching method
						Object propertySetterWrapper = new PropertySetterWrapper() {
							public void set(Object instance, Object value) {
								GeneratedInstanceInterceptor.setProperty(instance, propertyName, value);
							}
						};
						try {
							accessors.put(method, propertySetterWrapper.getClass().getMethod("set", new Class[] {Object.class, Object.class}));
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
			
			propertyByGetter = new HashMap<Method, String>();
			getterByProperty = new HashMap<String, Method>();
			for (Method method : accessors.keySet()) {
				String property = BeanUtils.findPropertyForMethod(method).getName();
				propertyByGetter.put(method, property);
				getterByProperty.put(property, method);
			}
			
			ownerByProperty = new HashMap<String, Class<?>>();
			for (String property : getterByProperty.keySet()) {
				Class<?> owner = ReflectionTools.getOwnerOfMethod(clazz, property, new Class[] {});
				ownerByProperty.put(property, owner);
			}
			
			settersByGetter = new HashMap<Method, Map<Class<?>, Method>>();
			for (Method getter : accessors.keySet()) {
				Map<Class<?>, Method> setters = new HashMap<Class<?>, Method>();
				setters.put(getter.getReturnType(), accessors.get(getter));
				for (Method method : clazz.getMethods()) {
					if (method.getName().equals("set" + getter.getName().substring(3))) {
						if (method != accessors.get(getter)) {
							Class<?>[] parameterTypes = method.getParameterTypes();
							if (parameterTypes.length == 1 && !parameterTypes[0].equals(getter.getReturnType())) {
								setters.put(parameterTypes[0], method);
							}
						}
					} 
				}
				settersByGetter.put(getter, setters);
			}
			
			propertyByAccessor = new HashMap<Method, String>();
			for (Method getter : accessors.keySet()) {
				String property = BeanUtils.findPropertyForMethod(getter).getName();
				propertyByAccessor.put(getter, property);
				for (Method setter : settersByGetter.get(getter).values()) {
					propertyByAccessor.put(setter, property);
				}
			}
			
			try {
				for (Class<?> c = clazz; !c.equals(Object.class) && rawSetter == null; c = c.getSuperclass()) {
					rawSetter = clazz.getDeclaredMethod("set", String.class, Object.class);
				}
			} catch (Exception ex) {
			}
		}
		
		public Collection<Method> getDeepMethods() {
			return deepMethods;
		}
		
		public Class<?> getRepresentedClass() {
			return clazz;
		}
		
		public Collection<Method> getGetters() {
			return accessors.keySet();
		}
		
		public String getPropertyName(Method method) {
			return propertyByGetter.get(method);
		}
		
		public boolean doesSetterExist(Method getter) {
			return accessors.get(getter) != null;
		}
		
		public Method getCorrespondingSetter(String getterName, Class<?> argument) {
			return getSetterOfProperty(toProperty(getterName), argument);

		}
		
		public Method getCorrespondingSetter(Method getter) {
			return accessors.get(getter);
		}
		
		public Method getCorrespondingGetter(String setterName) {
			return getterByProperty.get(toProperty(setterName));
		}
		
		public Method getGetterOfProperty(String property) {
			return getterByProperty.get(property);
		}
		
		public boolean hasGetterOfProperty(String property) {
			return getterByProperty.get(property) != null;
		}
		
		public Method getSetterOfProperty(String property) {
			return accessors.get(getterByProperty.get(property));
		}
		
		public Method getSetterOfProperty(String property, Class<?> argument) {
			Method setter = null;
			Method getter = getterByProperty.get(property);
			if (getter != null) {
				setter = settersByGetter.get(getter).get(argument);
			}
			if (setter == null) {
				if (accessors.get(getter).getParameterTypes()[0].isAssignableFrom(argument)) {
					setter = accessors.get(getter);
				}
;			}
			return setter;
		}
		
		public Collection<Method> getDeclaredPublicMethods() {
			return declaredPublicMethods;
		}
		
		public Method getRawSetter() {
			return rawSetter;
		}
		
		public Collection<String> getPropertiesOfGetters() {
			return getterByProperty.keySet();
		}
		
		public Class<?> getOwnerOfMethod(String property) {
			return ownerByProperty.get(property);
		}
		
		private String toProperty(String accessor) {
			return accessor.substring(3, 4).toLowerCase().concat(accessor.length() > 4 ? accessor.substring(4) : "");
		}
	}
	
	private static Map<Class<?>, Descriptor> descriptors = new HashMap<Class<?>, Descriptor>();
	
	private static void checkInitialized(Class<?> clazz) {
		if (! descriptors.containsKey(clazz)) {
			descriptors.put(clazz, new Descriptor(clazz));
		}
	}
	
	/**
	 *  Creates a hash code based on the getters of an interface
	 * @param clazz
	 * @return
	 */
	public static <T> int getInterfaceHashCode(T instance, Class<T> basedUponThisInterface) {
		return Amass.makeHashCode(invokeGetters(instance, basedUponThisInterface));
	}
	// Since Java has dumb getters and setters, this helps match
	// a private field to it's corresponding getter and/or setter
	public static String capitalize(String s) {
	    if (s.length() == 0) return s;
	    return s.substring(0, 1).toUpperCase() + s.substring(1);
	}
	public static boolean isGetter(String s) {
		return s.startsWith("get");
	}
	public static boolean isSetter(String s) {
		return s.startsWith("set");
	}
	
	/*
	 *  Strip the get or set off a getter or setter and lower case
	 *  the name to reveal the underlying property name
	 * 
	 */
	public static String getPropertyNameOfAccessor(Method accessor) {
		Class<?> clazz = accessor.getDeclaringClass();
		checkInitialized(clazz);
		return descriptors.get(clazz).getPropertyName(accessor);
	}
	
	public static boolean isGetter(Method method) {
		return 
			(method.getName().startsWith("is") || method.getName().startsWith("get")) &&
			method.getReturnType() != void.class &&
			method.getParameterTypes().length == 0;
	}
	public static boolean isSetter(Method method) {
		return 
			method.getName().startsWith("set") &&
			method.getReturnType() == void.class &&
			method.getParameterTypes().length == 1;
	}
	public static boolean doesSetterExist(Method getter) {
		Class<?> clazz = getter.getDeclaringClass();
		checkInitialized(clazz);
		return descriptors.get(clazz).doesSetterExist(getter);
	}
	
	public static Method getCorrespondingSetter(Object instance, String getterName, Class argumentType) {
		Class<?> clazz = instance.getClass();
		checkInitialized(clazz);
		return descriptors.get(clazz).getCorrespondingSetter(getterName, argumentType);
	}
	
	public static Method getCorrespondingSetter(Method getter) {
		Class<?> clazz = getter.getDeclaringClass();
		checkInitialized(clazz);
		return descriptors.get(clazz).getCorrespondingSetter(getter);
	}
	
	public static Method getCorrespondingGetter(Object instance, String setterName) {
		Class<?> clazz = instance.getClass();
		checkInitialized(clazz);
		return descriptors.get(clazz).getCorrespondingGetter(setterName);
	}
	
	public static Method getGetterOfProperty(Class ofInterface, final String property) {
		checkInitialized(ofInterface);
		return descriptors.get(ofInterface).getGetterOfProperty(property);
	}
	
	public static boolean hasGetterOfProperty(Class ofInterface, final String property) {
		checkInitialized(ofInterface);
		return descriptors.get(ofInterface).hasGetterOfProperty(property);
	}
	
	public static Method getSetterOfProperty(Class ofInterface, String property) {
		checkInitialized(ofInterface);
		return descriptors.get(ofInterface).getSetterOfProperty(property);
	}
	
	public static String makeSetterName(String property) {
		return "set" + capitalize(property);
	}
	
	public static Collection<Method> getDeclaredPublicMethods(Class subject) {
		checkInitialized(subject);
		return descriptors.get(subject).getDeclaredPublicMethods();
	}
	
	public static boolean doesImplementOrExtend(Class doesClass, Class implementOrExtendThisClass) {
		return implementOrExtendThisClass.isAssignableFrom(doesClass);
	}
	
	public static boolean doesImplementOrExtend(final Class[] doesOneOfThese, final Class matchOrImplementThisInterface) {
		for (Class clazz : doesOneOfThese) {
			if (doesImplementOrExtend(clazz, matchOrImplementThisInterface)){
				return true;
			}
		}
		return false;
	}
	
	/**
	 *  Returns the interface in the list that is mostly closest to the given class/interface.
	 *  The order of search is 1) see if the class matches one of the given interfaces,
	 *  2) see if one of the class's implemented interfaces and interfaces of those interfaces implements
	 *  one of the given interfaces, 3) rerun this method on the given class's parent class.
	 *  
	 *  This test should be used when there is one obvious answer, not when the answer may be ambiguous,
	 *  in which case the returned class will not be meaningful.
	 * @param doesClassOrInterface
	 * @param implementOneOfTheseInterfaces
	 * @return
	 */
	public static Class whichIsImplemented(final Class doesClassOrInterface, final Collection<? extends Class> implementOneOfTheseInterfaces)
	{
		Class answer = Filter.grepSingleOrNull(new Predicate<Class>() {
			public boolean f(Class implementThisInterface) {
				return doesClassOrInterface.equals(implementThisInterface);
			}},
			implementOneOfTheseInterfaces);
		if (answer != null)
			return answer;
		
		answer = Filter.grepSingleOrNull(new Filter.NotNullPredicate<Class>(),
				Transform.map(new Unary<Class, Class>() {
					public Class f(Class anInterface) {
						return whichIsImplemented(anInterface, implementOneOfTheseInterfaces);
					}},
					Arrays.asList(doesClassOrInterface.getInterfaces())));
		if (answer != null)
			return answer;
		
		return  
			(doesClassOrInterface.getSuperclass() != null && !doesClassOrInterface.getSuperclass().equals(Object.class))
					? whichIsImplemented(doesClassOrInterface.getSuperclass(), implementOneOfTheseInterfaces)
					: null;
	}
	
	/**
	 *  Call the default constructor
	 * @param <T>
	 * @param type
	 * @param partitionDimensionName
	 * @return
	 */
	public static<T> T carefreeConstructor(Class<T> type) {
		try {
			return type.getConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}
	/**
	 *  Call a one-argument constructor
	 * @param <T>
	 * @param type
	 * @param partitionDimensionName
	 * @param argument
	 * @return
	 */
	public static<T> T carefreeConstructor(Class<T> type, String partitionDimensionName, Object argument) {
		try {
			return type.getConstructor(new Class[] {argument.getClass()}).newInstance(new Object[] {argument});
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}
	
	public static<T> Collection<Method> getNullFields(final T checkMembersOfThis, Class<T> basedUponThisInterface) {
		return
			new MethodGrepper<T>() {
				public boolean invokableMemberPredicate(Method getter) {
					try {
						return getter.invoke(checkMembersOfThis) == null;
					} catch(Exception e) {
						throw new RuntimeException(e);
					}
				}
			}.grepGetters(basedUponThisInterface);
	}
	
	public static<T> Collection<Object> invokeGetters(final T instance, Class<T> basedUponThisInterface) {
		checkInitialized(basedUponThisInterface);
		return invokeGetters(instance, descriptors.get(basedUponThisInterface).getGetters());	
	}
	
	public static<T> Collection<Object> invokeGetters(final T instance, Collection<Method> getters) {
		return Transform.map(new Unary<Method, Object>() {
			public Object f(Method method) {
				try {
					return method.invoke(instance, new Object[] {});
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}},
			getters);	
	}
	
	public static Object invokeGetter(Object instance, String propertyName)
    {
		Class<?> clazz = instance.getClass();
		checkInitialized(clazz);
		Method getter = descriptors.get(clazz).getGetterOfProperty(propertyName);
		// try to match on the value's type
    	try {
    		return getter.invoke(instance, new Object[] {});
    	}
    	catch (Exception e) {
	    	throw new RuntimeException(String.format("Error invoking %s of class %s", getter.getName(), instance.getClass()), e);
    	}
    }
	
	public static void invokeSetter(Object instance, String propertyName, Object value)
    {
		Class<?> clazz = instance.getClass();
		checkInitialized(clazz);
		
		// try to match on the value's type
		Method setter = descriptors.get(clazz).getSetterOfProperty(propertyName, value.getClass());
		if (setter != null) {
			try {
				setter.invoke(instance, new Object[] {value});
				return;
			} catch (Exception ex) {
			}
    	}
		// handle our CGLib classes that don't have a setter interface
	    // TODO poor solution
	    try {
			descriptors.get(clazz).getRawSetter().invoke(instance, new Object[] {propertyName, value});
		} catch (Exception ex) {
			throw new RuntimeException("Exception calling method " + makeSetterName(propertyName) + " with a value of type " + value.getClass(), ex);
		}
    }
	
	public static<T> Collection<Method> getGetters(final Class<T> ofThisInterface) {
		checkInitialized(ofThisInterface);
		return descriptors.get(ofThisInterface).getGetters();
	}
	
	public static<T> Collection<String> getPropertiesOfGetters(final Class<T> ofThisInterface) {
		checkInitialized(ofThisInterface);
		return descriptors.get(ofThisInterface).getPropertiesOfGetters();
	}
	
	public static<T> DiffCollection getEqualFields(final T expected, final T actual, Class<T> basedUponThisInterface) { 
		return compareFields(expected, actual, basedUponThisInterface, new Filter.EqualFunction<Object>());
	}
	
	public static<T> DiffCollection getDifferingFields(final T expected, final T actual, Class<T> basedUponThisInterface) {
		return compareFields(expected, actual, basedUponThisInterface, new Filter.UnequalFunction<Object>());
	}
	public static class Diff
	{
		private Method method;
		private Object fieldValueOfInstance1;
		private Object fieldValueOfInstance2;
		public Diff(Method method, Object fieldValueOfInstance1, Object fieldValueOfInstance2)
		{
			this.method = method;
			this.fieldValueOfInstance1 = fieldValueOfInstance1;
			this.fieldValueOfInstance2 = fieldValueOfInstance2;
		}
		public Object getFieldOfInstance1() {
			return fieldValueOfInstance1;
		}
		public Object getFieldOfInstance2() {
			return fieldValueOfInstance2;
		}
		public Method getMethod() {
			return method;
		}
	
		public String toString() {			
			return String.format("Method: %s, FieldValue1: %s (hash: %s), FieldValue2: %s (hash: %s)", method, fieldValueOfInstance1!=null?fieldValueOfInstance1:"null", fieldValueOfInstance1!=null?fieldValueOfInstance1.hashCode():0, fieldValueOfInstance2!=null?fieldValueOfInstance2:"null", fieldValueOfInstance2!=null?fieldValueOfInstance2.hashCode():0);
		}
	}
	public static class DiffCollection extends ArrayList<Diff>
	{
		private static final long serialVersionUID = 1L;

		public DiffCollection(Collection<Diff> c) {
			super(c);
		}
		public boolean containsGetter(final String getterName)
		{
			return Filter.isMatch(new Predicate<Diff>() {
				public boolean f(Diff diff) {
					return diff.getMethod().getName().equals(getterName);
				}
			}, this);
		}
		@Override
		public String toString() {
			return String.format("The following fields differ %s", Amass.joinByToString(new ConcatStrings<Diff>(", "), this));
		}
	}
	
	private static <T> DiffCollection compareFields(final T expected, final T actual, Class<T> basedUponThisInterface, final Filter.BinaryPredicate<Object, Object> compare) {
		if (expected == null || actual == null)
			throw new RuntimeException(String.format("Expected and/or actual instance are/is null. Expected null? %s, Actual null? %s", expected==null, actual==null ));
		
		return  new DiffCollection(Transform.map(new Unary<Method, Diff>() {
				public Diff f(Method getter) {
					try {
						return new Diff(getter, wrapIfNeeded(getter.invoke(expected)), wrapIfNeeded(getter.invoke(actual)));
					} catch(Exception e) {
						throw new RuntimeException(e);
					}
				}},
				new MethodGrepper<T>() {
					public boolean invokableMemberPredicate(Method getter) {
						try {
							
							final Object expectedWrapped = wrapIfNeeded(getter.invoke(expected));
							final Object actualWrapped = wrapIfNeeded(getter.invoke((actual)));
							if (expectedWrapped == null || actualWrapped == null)
								return (expectedWrapped==null ^ actualWrapped==null);
							return compare.f(expectedWrapped, actualWrapped);
						} catch(Exception e) {
							throw new RuntimeException(e);
						}
					}
					
				}.grepGetters(basedUponThisInterface)));
	}
	@SuppressWarnings("unchecked")
	private static Object wrapIfNeeded(Object fieldValue)
	{
		if (fieldValue instanceof Collection)
			return new HashSet((Collection)fieldValue);
		return fieldValue;
	}
	
	
	private static class MethodGrepper<T> {
		public Collection<Method> grepGetters(Class<T> basedUponThisInterface) {
			checkInitialized(basedUponThisInterface);
			Collection<Method> deepMethods = descriptors.get(basedUponThisInterface).getDeepMethods();
			Collection<Method> getters = Filter.grep(new Predicate<Method>() {
				public boolean f(Method method) {
					try {
						return
							!method.getDeclaringClass().equals(Object.class) &&
							isGetter(method) &&
							invokableMemberPredicate(method);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}, deepMethods);
			return getters;
		}
		
		
		/**
		 *  Override this to filter for specific methods
		 * @param getter
		 * @return
		 */
	
		public boolean invokableMemberPredicate(Method getter) { return true; }
	}
	
	// Returns uniquely named methods belong to the given class and its ancestors.
	private static Collection<Method> getDeepMethods(Class<?> clazz) {
		return Filter.grepUnique(
			new Unary<Method, String>() {
				public String f(Method method) {
					return method.getName();
				}
			}, Transform.flatMap(new Unary<Class<?>, Collection<Method>>() {

				public Collection<Method> f(Class<?> clazz) {
					return Arrays.asList(clazz.getMethods());
				}
			}, Transform.flatten(Arrays.asList((Class<?>[])new Class[] { clazz }), getAncestors(clazz))));
	}

	public static Class<?> getPropertyType(final Class<?> ofThisInterface, String propertyName) {
		checkInitialized(ofThisInterface);
		return descriptors.get(ofThisInterface).getGetterOfProperty(propertyName).getReturnType();
	}
	
	public static Class<?> getCollectionItemType(final Class<?> clazz, final String propertyName) {	
		checkInitialized(clazz);
		Class ofThisInterface = descriptors.get(clazz).getOwnerOfMethod(propertyName);
		final Method getter = descriptors.get(ofThisInterface).getGetterOfProperty(propertyName);
		Type type = getter.getGenericReturnType();
	
		if (type instanceof ParameterizedType) {
			Type typeArgument = Atom.getFirstOrThrow(((ParameterizedType)type).getActualTypeArguments());
			try {
				return (Class)(typeArgument instanceof WildcardType
						? Atom.getFirstOrThrow(((WildcardType)typeArgument).getUpperBounds())
						: (typeArgument instanceof ParameterizedType)
							? ((ParameterizedType)typeArgument).getRawType()
							: typeArgument);
			}
			catch (ClassCastException e) {
				throw new RuntimeException(String.format("For Interface: %s, Property Name %s, expected ParameterizedType or WildcardType but got %s", ofThisInterface, propertyName, typeArgument), e);
			}
		}
		else
			throw new RuntimeException(String.format("For Interface: %s, Property Name %s, expected ParameterizedType or WildcardType but got %s", ofThisInterface, propertyName, type));	
	}
	
	public static Class getOwnerOfMethod(final Class<?> clazz, final String propertyName) {
		checkInitialized(clazz);
		// Java magically erases generic information when referencing a method from a subclass,
		// extended interface, or implementation (naturally)
		// Extract the first owning interface or superclass if it exists
		return descriptors.get(clazz).getOwnerOfMethod(propertyName);
	}
	
	public static Method getMethodOfOwner(Method method) {
		Class owner = getOwnerOfMethod(method.getDeclaringClass(), method.getName(), method.getParameterTypes());
		try {
			return owner.getMethod(method.getName(), method.getParameterTypes());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private static Class getOwnerOfMethod(final Class<?> clazz, final String methodName, final Class[] parameterTypes) {
		final List<Class> classes = new ArrayList(getAncestors(clazz));
		Class ofThisInterface;
		ofThisInterface = 
			Filter.grepSingleOrNull(new Predicate<Class>() {
				public boolean f(Class c) {
					try {
						c.getDeclaredMethod(methodName, parameterTypes);
						return true;
					}
					catch (Exception e) {
						return false;
					}
				}
			}, classes);
		if (ofThisInterface == null)
			ofThisInterface = clazz;
		return ofThisInterface;
	}
	
	private static Collection<Class<?>> getAncestors(Class<?> clazz) {
		return Transform.flatten(new Collection[] {
				clazz.getSuperclass() != null ? getAncestors(clazz.getSuperclass()) : new ArrayList<Class<?>>(),
				Transform.flatMap(new Unary<Class<?>, Collection<Class<?>>>() {
					public Collection<Class<?>> f(Class<?> interfaceClass) {
						return Transform.flatten(new Collection[] {Collections.singleton(interfaceClass), getAncestors(interfaceClass)});
					}
				}, Arrays.asList((Class<?>[])clazz.getInterfaces()))});
	}
	
	/**
	 *  Returns the given property's return type or the type of item in the collection if its return type 
	 *  is a collection.
	 */
	public static Class<?> getPropertyTypeOrPropertyCollectionItemType(final Class<?> ofThisInterface, String propertyName) {
		return isCollectionProperty(ofThisInterface, propertyName)
			? getCollectionItemType(ofThisInterface, propertyName)
			: getPropertyType(ofThisInterface, propertyName);
	}
	
	public static boolean isCollectionProperty(final Class<?> ofThisInterface, String propertyName) {
		return ReflectionTools.doesImplementOrExtend(
					ReflectionTools.getPropertyType(ofThisInterface, propertyName),
					Collection.class); 
	}
	
	public static Collection<String> getPropertiesOfScalarGetters(final Class<?> ofThisInterface) {
		return Filter.grep(new Predicate<String>() {
			public boolean f(String propertyName) {
				return !isCollectionProperty(ofThisInterface, propertyName);
		}},
		getPropertiesOfGetters(ofThisInterface));
	}
	
	public static Collection<String> getPropertiesOfCollectionGetters(final Class<?> ofThisInterface) {
		return Filter.grep(new Predicate<String>() {
			public boolean f(String propertyName) {
				return isCollectionProperty(ofThisInterface, propertyName);
		}},
		getPropertiesOfGetters(ofThisInterface));
	}
	public static Collection<Method> getCollectionGetters(final Class<?> ofThisInterface) {
		return Filter.grep(new Predicate<Method>() {
			public boolean f(Method getter) {
				return isCollectionProperty(ofThisInterface, getPropertyNameOfAccessor(getter));
		}},
		getGetters(ofThisInterface));
	}
	
	public static Collection<String> getPropertiesOfPrimitiveGetters(final Class<?> ofThisInterface) {
		return getPrimitiveGetters(ofThisInterface);
	}
	public static Collection<String> getPrimitiveGetters(final Class<?> ofThisInterface) {
		return Filter.grep(new Predicate<String>() {
			public boolean f(String propertyName) {
				return PrimitiveUtils.isPrimitiveClass(getPropertyType(ofThisInterface, propertyName));
		}},
		getPropertiesOfGetters(ofThisInterface));
	}
	
	public static Collection<String> getPropertiesOfComplexGetters(final Class<?> ofThisInterface) {
		return Transform.map(new Unary<Method,String>() { 
			public String f(Method method) {
				return ReflectionTools.getPropertyNameOfAccessor(method);
		}},
		getComplexGetters(ofThisInterface));
	}
	
	public static Collection<Method> getComplexGetters(final Class<?> ofThisInterface) {
		return Filter.grep(new Predicate<Method>() {
			public boolean f(Method getter) {
				return !PrimitiveUtils.isPrimitiveClass(getPropertyType(ofThisInterface, ReflectionTools.getPropertyNameOfAccessor(getter)));
		}},
		getGetters(ofThisInterface));
	}
	
	public static Collection<String> getPropertiesOfGivenType(final Class<?> representedInterface, final Class<?> propertyType) {
		return Filter.grep(new Predicate<String>() {
			public boolean f(String propertyName) {
				return propertyType.equals(ReflectionTools.getPropertyType(representedInterface, propertyName));
			}
		},  ReflectionTools.getPropertiesOfGetters(representedInterface));
	}
	
	/**
	 * Gets all non-primitive classes nested in the given classes.
	 * @param representedInterfaces
	 * @return The given classes and nested classes.
	 */
	@SuppressWarnings("unchecked")
	public static Collection<Class<?>> getUniqueComplexPropertyTypes(final Collection representedInterfaces) {
		if (representedInterfaces.size() == 0)
			return Collections.emptyList();
		
		final Collection<Class<?>> uniqueComplexPropertyTypes = getUniqueComplexPropertyTypes(
					Filter.getUnique(
						Transform.flatten(Transform.map(new Unary<Class<?>, Collection<Class<?>>>() {
							public Collection<Class<?>> f(Class<?> representedInterface) {
								return getInterfacesOfComplexGetters(representedInterface);
						}}, representedInterfaces))));
		// Return a unique collection of the given representedInterfaces merged with the 
		// flattened deep collection of all their contained complex property types
		return Filter.getUnique(Transform.flatten((Collection<Class<?>>[])new Collection[] {
			representedInterfaces,
			uniqueComplexPropertyTypes}));
	}

	// Get the return types of getters or the underlying type of a collection getter for types that are not primitive
	public static Collection<Class<?>> getInterfacesOfComplexGetters(final Class representedInterface) {
		return Filter.getUnique(
			Transform.map(new Unary<String,Class<?>>() {
				public Class<?> f(String propertyName) {
					return ReflectionTools.getPropertyTypeOrPropertyCollectionItemType(representedInterface, propertyName);
				}},		
				Filter.grep(new Predicate<String>() {
					public boolean f(String property) {
						return !(ReflectionTools.isCollectionProperty(representedInterface, property)) ||
						 !PrimitiveUtils.isPrimitiveClass(ReflectionTools.getCollectionItemType(representedInterface, property));
					}
				},
				ReflectionTools.getPropertiesOfComplexGetters(representedInterface))));
	}
	
	public static boolean isComplexCollectionItemProperty(final Class representedInterface, String property) {
		return ReflectionTools.isCollectionProperty(representedInterface, property) &&
			 !PrimitiveUtils.isPrimitiveClass(ReflectionTools.getCollectionItemType(representedInterface, property));
	}
	
	/**
	 *  I'm not sure if this done any good since getMethods should only return the owned methods
	 * @param clazz
	 * @return
	 */
	public static Collection<Method> getOwnedMethods(final Class<?> clazz) {
		return Filter.grep(new Predicate<Method>() {
			public boolean f(Method method) {
				return method.getDeclaringClass().equals(clazz);
		}}, Arrays.asList(clazz.getMethods()));
	}
}
