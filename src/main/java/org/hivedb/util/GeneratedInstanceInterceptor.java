/**
 * 
 */
package org.hivedb.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import net.sf.cglib.core.DefaultNamingPolicy;
import net.sf.cglib.core.KeyFactory;
import net.sf.cglib.core.NamingPolicy;
import net.sf.cglib.core.Predicate;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.LazyLoader;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.DebugMap;
import org.hivedb.util.functional.Delay;

import sun.reflect.ReflectionFactory.GetReflectionFactoryAction;

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
	
	public static<T> Class<?> getGeneratedClass(final Class<T> clazz ) {
		Enhancer e = new Enhancer();
		e.setCallbackType(GeneratedInstanceInterceptor.class);
		e.setNamingPolicy(new ImplNamer(clazz));
		e.setSuperclass(Mapper.class);
		GeneratedInstanceInterceptor interceptor = new GeneratedInstanceInterceptor(clazz);	
		if (clazz.isInterface())
			e.setInterfaces(new Class[] {clazz, PropertySetter.class});
		else {
			List list = new ArrayList(Arrays.asList(clazz.getInterfaces()));
			list.add(PropertySetter.class);
			list.add(MapBacked.class);
			Class[] copy = new Class[list.size()];
			list.toArray(copy);
			e.setInterfaces(copy);
		}
		Class<?> generatedClass = e.createClass();
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
		PropertySetter<T> instance = (PropertySetter<T>) newInstance(clazz);
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
			return hashCode(obj);
		}
		else if ( name.equals("equals")) {
			return obj.hashCode() == args[0].hashCode();
		}
		else if ( name.equals("toString")) {
			return new DebugMap<Object, Object>(dictionary, true).toString() + "###("+dictionary.hashCode()+")";
		}
			
		return retValFromSuper;
	}

	private Object hashCode(Object obj) {
		return Amass.makeHashCode(ReflectionTools.invokeGetters(obj, clazz));
	}
	
	static class ImplNamer extends DefaultNamingPolicy {
		private Class representedInterface;
		public ImplNamer(Class representedInterface) {
			this.representedInterface = representedInterface;
		}
		public String getClassName(String prefix, String source, Object key, Predicate names) {
		
			// The RepresentedInterface comes through here twice. I only accept the first pass through
			// where the key is not equal to the represented interface name. I don't understand
			// the CGLib implementation yet
			return representedInterface.getAnnotation(GeneratedClass.class) != null
						? removeClass(representedInterface.getCanonicalName()) + ((GeneratedClass)  representedInterface.getAnnotation(GeneratedClass.class)).value()
						: super.getClassName(prefix, source, key, names);

		}
		private String removeClass(String prefix) {
			return prefix.substring(0,prefix.lastIndexOf(".")+1); // maintains the final dot
		}
	}
}