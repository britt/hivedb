/**
 * 
 */
package org.hivedb.util.classgen;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.EntityId;
import org.hivedb.util.DeepHashCode;
import org.hivedb.util.PropertyAccessor;
import org.hivedb.util.PropertyChangeListenerRegistrar;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.DebugMap;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.Map;

public class GeneratedInstanceInterceptor implements  MethodInterceptor {
	
	private Class clazz;	
	private Map map = new Hashtable();
	private PropertyChangeSupport propertySupport;
	public GeneratedInstanceInterceptor(Class clazz) {
		this.clazz = clazz;
	}
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
		String name = method.getName();
		Implementor implementor = new Implementor(obj);
		
		// Fake setters
		if( name.startsWith("set") && args.length == 1 && method.getReturnType() == Void.TYPE ) {
			char propName[] = name.substring("set".length()).toCharArray();
			propName[0] = Character.toLowerCase( propName[0] );
			implementor.set(new String( propName ), args[0]);
			return null;
		}
		else if( name.startsWith("get") && args.length == 0 ) {
			char propName[] = name.substring("get".length()).toCharArray();
			propName[0] = Character.toLowerCase( propName[0] );
			return implementor.get(new String( propName ));
		} else
		  return method.invoke(implementor, args);		
	}

	/**
	 *  Sets the property of an instance by using the setter if it exists or
	 *  by using the PropertySetter interface for generated classes
	 * @param instance
	 * @param property
	 * @param value
	 */
	public static void setProperty(Object instance, String property, Object value) {
		if (ReflectionTools.doesRealSetterExist(ReflectionTools.getGetterOfProperty(instance.getClass(), property)))
			ReflectionTools.invokeSetter(instance, property, value);
		else if (instance instanceof PropertyAccessor)
			((PropertyAccessor)instance).set(property, value);
		else
			throw new HiveRuntimeException(String.format("No way to inoke setter of class %s, property %s", instance.getClass(), property));
	}
	
	private class Implementor implements GeneratedImplementation, PropertyAccessor, PropertyChangeListenerRegistrar, DeepHashCode {
		Object obj = null;
		
		public Implementor(Object obj) {
			this.obj = obj;
		}
	
		public boolean equals(Object argument) {
			return argument == null ? false : new Integer(shallowHashCode(obj)).equals(new Integer(shallowHashCode(argument)));
		}
		
		public void set(String propertyName, Object value) {
			if (value != null)
				map.put(propertyName, value);
		}
		public Object get(String property) {
			return map.get(property);
		}
	
		@Override
		public int hashCode() {
			return shallowHashCode(obj);
		}
		@SuppressWarnings("unchecked")
		public int shallowHashCode(Object obj) {
			Method idGetter = Atom.getFirstOrNull(AnnotationHelper.getAllMethodsWithAnnotation(clazz, EntityId.class));
			if (idGetter != null)
				try {
					return Amass.makeHashCode(new Object[]{idGetter.invoke(obj, new Object[] {})});
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			
			return Amass.makeHashCode(ReflectionTools.invokeGetters(obj, clazz));
		}
		public int deepHashCode(Object obj) {
			return Amass.makeHashCode(ReflectionTools.invokeGetters(obj, clazz));
		}
		
		
		public void addPropertyChangeListener(PropertyChangeListener listener) {          
		    propertySupport.addPropertyChangeListener(listener);        
		}
		  
		public void removePropertyChangeListener(PropertyChangeListener listener) {
			propertySupport.removePropertyChangeListener(listener);
		}
	
		public Map retrieveMap() {
			return map;
		}
	
		public Class retrieveUnderlyingInterface() {
			return clazz;
		}
		
		public String toString() {
			return new DebugMap<Object, Object>(map, true).toString();
		}
	}
	
}