/**
 * 
 */
package org.hivedb.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Hashtable;
import java.util.Map;

import net.sf.cglib.core.DefaultNamingPolicy;
import net.sf.cglib.core.Predicate;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.GeneratedClass;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.DebugMap;

public class GeneratedInstanceInterceptor implements MethodInterceptor {
	
	Class clazz;
	public GeneratedInstanceInterceptor(Class clazz) {
		this.clazz = clazz;
	}
	private PropertyChangeSupport propertySupport;
	   
	public void addPropertyChangeListener(PropertyChangeListener listener) {          
	    propertySupport.addPropertyChangeListener(listener);        
	}
	  
	public void removePropertyChangeListener(PropertyChangeListener listener) {
		propertySupport.removePropertyChangeListener(listener);
	}
	
	public static<T> Class<? extends T> getGeneratedClass(final Class<T> clazz ) {
		// Only generate for interfaces.
		// Implementations can add whatever functionality they need, and so don't warrant a generated class
		if (!clazz.isInterface())	
			return clazz;
		Enhancer e = new Enhancer();
		e.setCallbackType(GeneratedInstanceInterceptor.class);
		e.setNamingPolicy(new ImplNamer(clazz));
		e.setSuperclass(Mapper.class);
		GeneratedInstanceInterceptor interceptor = new GeneratedInstanceInterceptor(clazz);	
		e.setInterfaces(new Class[] {clazz, PropertySetter.class, GeneratedImplementation.class});
		Class<? extends T> generatedClass = e.createClass();
		Enhancer.registerCallbacks(generatedClass, new Callback[] {interceptor});
		return generatedClass;
	}
	public static<T> T newInstance( Class<T> clazz ){
		try{
			Object instance = getGeneratedClass(clazz).newInstance();
			return (T) instance;
		}catch( Throwable e ){
			 e.printStackTrace();
			 throw new RuntimeException(e.getMessage(), e);
		}
	}
	public static<T> T newInstance( Class<T> clazz, Map<String, Object> prototype ){
		PropertySetter instance = (PropertySetter) newInstance(clazz);
		for (String propertyName : ReflectionTools.getPropertiesOfGetters((Class<?>)clazz))
			instance.set(propertyName, prototype.get(propertyName));
		return (T) instance;
	}
	
	Map<Object,Object> dictionary = new Hashtable<Object,Object>();
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		Object retValFromSuper = null;
		try {
			if (!Modifier.isAbstract(method.getModifiers())) {
				retValFromSuper = proxy.invokeSuper(obj, args);
			}
		} 
		finally {}
		
		String name = method.getName();
		if( name.equals("getMap") )
			return retValFromSuper;
		
		Map<Object, Object> dictionary = ((MapBacked)obj).getMap();
		
		if( name.equals("addPropertyChangeListener"))
			addPropertyChangeListener((PropertyChangeListener)args[0]);
		else if ( name.equals( "removePropertyChangeListener" ) )
			removePropertyChangeListener((PropertyChangeListener)args[0]);

		if( name.equals("set")) {
			String propName = (String) args[0];
			dictionary.put(new String( propName ), args[1]);
		}
		
		else if( name.startsWith("set") && args.length == 1 && method.getReturnType() == Void.TYPE ) {
			char propName[] = name.substring("set".length()).toCharArray();
			propName[0] = Character.toLowerCase( propName[0] );
			dictionary.put(new String( propName ), args[0]);
		}
		else if ( name.equals("getAsMap")) {
			return dictionary;
		}
		else if( name.startsWith("get") && args.length == 0 ) {
			char propName[] = name.substring("get".length()).toCharArray();
			propName[0] = Character.toLowerCase( propName[0] );
			Object propertyValue = dictionary.get(new String( propName ));
			if (propertyValue != null)
				return propertyValue;
		}
		else if ( name.equals("hashCode")) {
			return idHashCode(obj);
		}
		else if ( name.equals("deepHashCode")) {
			return deepHashCode(obj);
		}
		else if ( name.equals("equals")) {
			return idHashCode(obj) == idHashCode(args[0]);
		}
		else if ( name.equals("toString")) {
			return new DebugMap<Object, Object>(dictionary, true).toString() + "###("+dictionary.hashCode()+")";
		}
			
		return retValFromSuper;
	}

	private Object idHashCode(Object obj) {
		Method idGetter = Atom.getFirstOrNull(AnnotationHelper.getAllMethodsWithAnnotation(clazz, EntityId.class));
		if (idGetter != null)
			try {
				return Amass.makeHashCode(new Object[]{idGetter.invoke(obj, new Object[] {})});
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		
		return Amass.makeHashCode(ReflectionTools.invokeGetters(obj, clazz));
	}
	private Object deepHashCode(Object obj) {
		return Amass.makeHashCode(ReflectionTools.invokeGetters(obj, clazz));
	}
	
	/**
	 *  Sets the property of an instance by using the setter if it exists or
	 *  by using the PropertySetter interface for generated classes
	 * @param instance
	 * @param property
	 * @param value
	 */
	public static void setProperty(Object instance, String property, Object value) {
		if (ReflectionTools.doesSetterExist(ReflectionTools.getGetterOfProperty(instance.getClass(), property)))
			ReflectionTools.invokeSetter(instance, property, value);
		else if (instance instanceof PropertySetter)
			((PropertySetter)instance).set(property, value);
		else
			throw new HiveRuntimeException(String.format("No way to inoke setter of class %s, property %s", instance.getClass(), property));
	}
	
	static class ImplNamer extends DefaultNamingPolicy {
		private Class representedInterface;
		public ImplNamer(Class representedInterface) {
			this.representedInterface = representedInterface;
		}
		public String getClassName(String prefix, String source, Object key, Predicate names) {
			return representedInterface.getAnnotation(GeneratedClass.class) != null
						? removeClass(representedInterface.getCanonicalName()) + ((GeneratedClass)  representedInterface.getAnnotation(GeneratedClass.class)).value()
						: super.getClassName(prefix, source, key, names);

		}
		private String removeClass(String prefix) {
			return prefix.substring(0,prefix.lastIndexOf(".")+1); // maintains the final dot
		}
	}
}