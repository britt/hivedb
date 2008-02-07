package org.hivedb.util;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.hivedb.annotations.Validate;
import org.hivedb.services.ClassDaoService;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Joiner.ConcatStrings;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;

public class ReflectionTools {
	
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
	 *  the name to rveal the underlying property name
	 * 
	 */
	public static String getPropertyNameOfAccessor(Method accessor)
	{
		return BeanUtils.findPropertyForMethod(accessor).getName();
//		return accessor.getName().substring(3,4).toLowerCase()
//			+ accessor.getName().substring(4);
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
		try {
			return BeanUtils.findPropertyForMethod(getter).getWriteMethod() != null;
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}
	}
	public static Method getCorrespondingSetter(Object instance, String getterName, Class argumentType) {
		try {
			return instance.getClass().getMethod("set" + getterName.substring(3), new Class[] {argumentType});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public static Method getCorrespondingSetter(Method getter) {
		try {
			return getter.getDeclaringClass().getMethod("set" + getter.getName().substring(3), new Class[] {getter.getReturnType()});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public static Method getCorrespondingGetter(Object instance, String setterName) {
		try {
			return instance.getClass().getMethod("get" + setterName.substring(3), new Class[] {});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Method getGetterOfProperty(Class ofInterface, final String property) {
		try {
			return Filter.grepSingle(new Predicate<Method>() {
				public boolean f(Method method) {
					return getPropertyNameOfAccessor(method).equals(property);
				}
			},
			getGetters((Class<Object>)ofInterface));
			//return BeanUtils.getPropertyDescriptor(ofInterface, property).getReadMethod();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static boolean hasGetterOfProperty(Class ofInterface, final String property) {
		try {
			return Filter.isMatch(new Predicate<Method>() {
				public boolean f(Method method) {
					return getPropertyNameOfAccessor(method).equals(property);
				}
			},
			getGetters((Class<Object>)ofInterface));
			//return BeanUtils.getPropertyDescriptor(ofInterface, property).getReadMethod() != null;
		} catch (Exception e) {
			return false;
		}
	}
	
	public static Method getSetterOfProperty(Class ofInterface, String property) {
		try {
			return ofInterface.getMethod(makeSetterName(property), new Class[] {
				getGetterOfProperty(ofInterface, property).getReturnType()
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String makeSetterName(String property) {
		return "set" + capitalize(property);
	}
	
	public static Collection<Method> getDeclaredPublicMethods(Class subject) {
		return Filter.grepAgainstList( // get declared in the class and public
			   Arrays.asList(subject.getMethods()),						   
			   Arrays.asList(subject.getDeclaredMethods()));
	}
	public static boolean doesImplementOrExtend(Class doesClass, Class implementOrExtendThisClass)
	{
		return doesClass.equals(implementOrExtendThisClass) ||
			doesImplementOrExtend(doesClass.getInterfaces(), implementOrExtendThisClass) ||
				(doesClass.getSuperclass() != null &&
				!doesClass.getSuperclass().equals(Object.class) &&	
				 doesImplementOrExtend(doesClass.getSuperclass(), implementOrExtendThisClass));
		 
	}
	
	public static boolean doesImplementOrExtend(final Class[] doesOneOfThese, final Class matchOrImplementThisInterface)
	{
		return Filter.isMatch(new Predicate<Class>() {
			
			public boolean f(Class anInterface) {
				return anInterface.equals(matchOrImplementThisInterface) || 
						doesImplementOrExtend(anInterface.getInterfaces(), matchOrImplementThisInterface);
					
			}},
			doesOneOfThese);
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
		return invokeGetters(instance, getGetters(basedUponThisInterface));	
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
		final String getterName = "get"+capitalize(propertyName);
		// try to match on the value's type
    	try {
    		return instance.getClass().getMethod(getterName, new Class[] {})
    			.invoke(instance, new Object[] {});
    	}
    	catch (Exception e) {
	    	throw new RuntimeException(String.format("Error invoking %s of class %s", getterName, instance.getClass()), e);
    	}
    }
	
	public static void invokeSetter(Object instance, String propertyName, Object value)
    {
		final String setterName = makeSetterName(propertyName);
		// try to match on the value's type
    	try {
    		instance.getClass().getMethod(setterName, new Class[] {value.getClass()})
    			.invoke(instance, new Object[] {value});
    	}
    	catch (Exception exception) {
    		// iterate through all methods until a name-matched setter is found
    		try {
	    		getMethod(instance, setterName).invoke(instance, new Object[] {value});
    		}
	    	catch (Exception e)
	    	{
	    		// handle our CGLib classes that don't have a setter interface
	    		// TODO poor solution
	    		try {
					getMethod(instance, "set").invoke(instance, new Object[] {propertyName, value});
				} catch (Exception ex) {
					throw new RuntimeException("Exception calling method " + setterName 
							+ " with a value of type " + value.getClass(), e);
				}
	    	}
    	}
    }
	private static Method getMethod(Object instance, final String setterName) {
		return Filter.grepSingle(
			new Predicate<Method>() {	public boolean f(Method m) {
				return m.getName().equals(setterName);						
			}},
			getDeepMethods(instance.getClass()));
	}
	
	public static<T> Collection<Method> getGetters(final Class<T> ofThisInterface) {
		return new MethodGrepper<T>().grepGetters(ofThisInterface);
	}
	
	public static<T> Collection<String> getPropertiesOfGetters(final Class<T> ofThisInterface) {
		return Transform.map(new Unary<Method,String>() {
			public String f(Method method) {
				return getPropertyNameOfAccessor(method);
			}
		},
		new MethodGrepper<T>().grepGetters(ofThisInterface));
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
			return Filter.grep(new Predicate<Method>() {
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
				},
				getDeepMethods(basedUponThisInterface));
		}
		
		
		/**
		 *  Override this to filter for specific methods
		 * @param getter
		 * @return
		 */
		public boolean invokableMemberPredicate(Method getter) { return true; }
	}
	
	// Returns uniquely named methods belong to the given class and its ancestors.
	public static Collection<Method> getDeepMethods(Class<?> clazz) {
		return Filter.grepUnique(
			new Unary<Method, String>() {
				public String f(Method method) {
					return method.getName();
				}
			},
			Transform.flatMap(new Unary<Class<?>, Collection<Method>>() {

				public Collection<Method> f(Class<?> clazz) {
					return Arrays.asList(clazz.getMethods());
				}
			}, Transform.flatten(Arrays.asList((Class<?>[])new Class[] { clazz }), getAncestors(clazz))));
	}

	public static class AccessorGrepper<T> {
		public Collection<Method> grepAccessors(Class<? extends T> basedUponThisInterface) {
			return Filter.grep(new Predicate<Method>() {
					public boolean f(Method method) {
						try {
							return ReflectionTools.doesImplementOrExtend(
										method.getReturnType(),
										AccessorFunction.class) &&
								 invokableMemberPredicate(method);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				},
				getDeepMethods(basedUponThisInterface));
		}
		/**
		 *  Override this to filter for specific methods
		 * @param accessor
		 * @return
		 */
		public boolean invokableMemberPredicate(Method accessor) { return true; }
		
	}

	public static Class<?> getPropertyType(final Class<?> ofThisInterface, String propertyName) {
		try {
			return ofThisInterface.getMethod("get"+capitalize(propertyName), new Class[] {}).getReturnType();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public static Class<?> getCollectionItemType(final Class<?> clazz, final String propertyName) {	
		
		Class ofThisInterface = getOwnerOfMethod(clazz, propertyName);
	
		Type type;
		try {
			final Method getter = BeanUtils.getPropertyDescriptor(ofThisInterface, propertyName).getReadMethod();
			type = getter.getGenericReturnType();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	
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
		// Java magically erases generic information when referencing a method from a subclass,
		// extended interface, or implementation (naturally)
		// Extract the first owning interface or superclass if it exists
		
		return getOwnerOfProperty(clazz, propertyName, new Class[] {});
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
	
	private static Class getOwnerOfProperty(final Class<?> clazz, final String propertyName, final Class[] parameterTypes) {
		final List<Class> classes = new ArrayList(getAncestors(clazz));
		Class ofThisInterface;
		ofThisInterface = 
			Filter.grepSingleOrNull(new Predicate<Class>() {
				public boolean f(Class c) {
					try {
						c.getDeclaredMethod(BeanUtils.getPropertyDescriptor(c, propertyName).getReadMethod().getName(), parameterTypes);
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
