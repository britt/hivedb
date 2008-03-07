/**
 * 
 */
package org.hivedb.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Hashtable;
import java.util.Map;

import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.EntityId;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.DebugMap;

public class GeneratedInstanceInterceptor implements MethodInterceptor, InvocationHandler {
	
	Class clazz;
	Map<Object,Object> dictionary = new Hashtable<Object,Object>();
	private PropertyChangeSupport propertySupport;
	public GeneratedInstanceInterceptor(Class clazz) {
		this.clazz = clazz;
	}
	
	public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
		String name = method.getName();
		if (name.equals("getUnderlyingInterface"))
			return clazz;
		if( name.equals("getMap") )
			return dictionary;
		
		Map<Object, Object> dictionary = ((MapBacked)obj).getMap();
		
		if( name.equals("addPropertyChangeListener"))
			addPropertyChangeListener((PropertyChangeListener)args[0]);
		else if ( name.equals( "removePropertyChangeListener" ) )
			removePropertyChangeListener((PropertyChangeListener)args[0]);

		if( name.equals("set")) {
			String propName = (String) args[0];
			if (args[1] != null)
				dictionary.put(new String( propName ), args[1]);
		}
		else if( name.startsWith("set") && args.length == 1 && method.getReturnType() == Void.TYPE ) {
			char propName[] = name.substring("set".length()).toCharArray();
			propName[0] = Character.toLowerCase( propName[0] );
			if (args[0] != null)
				dictionary.put(new String( propName ), args[0]);
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
			return idHashCode(obj).equals(idHashCode(args[0]));
		}
		else if ( name.equals("toString")) {
			return new DebugMap<Object, Object>(dictionary, true).toString() + "###("+dictionary.hashCode()+")";
		}
			
		return null;
	}
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		return invoke(obj, method, args);
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
		if (ReflectionTools.doesRealSetterExist(ReflectionTools.getGetterOfProperty(instance.getClass(), property)))
			ReflectionTools.invokeSetter(instance, property, value);
		else if (instance instanceof PropertySetter)
			((PropertySetter)instance).set(property, value);
		else
			throw new HiveRuntimeException(String.format("No way to inoke setter of class %s, property %s", instance.getClass(), property));
	}
	
	public void addPropertyChangeListener(PropertyChangeListener listener) {          
	    propertySupport.addPropertyChangeListener(listener);        
	}
	  
	public void removePropertyChangeListener(PropertyChangeListener listener) {
		propertySupport.removePropertyChangeListener(listener);
	}

	
}