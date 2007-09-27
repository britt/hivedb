package org.hivedb.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.hivedb.util.functional.Generator;

public class GenerateInstance<T> implements Generator<T> {
	
	private final int COLLECTION_SIZE = 3;
	private Class<T> interfaceToImplement;
	public GenerateInstance(Class<T> interfaceToImplement)
	{
		this.interfaceToImplement = interfaceToImplement;
	}
	
	@SuppressWarnings("unchecked")
	public T f() {
		T instance = (T)Interceptor.newInstance( interfaceToImplement );
		    
	    for (Method getter : ReflectionTools.getGetters(interfaceToImplement))
	    {
	    	String propertyName = ReflectionTools.getPropertyNameOfAccessor(getter);
	    	Class<Object> clazz = (Class<Object>) getter.getReturnType();
			if (ReflectionTools.doesImplementOrExtend(clazz, Collection.class)) {
	    		Class<Object> collectionItemClass = (Class<Object>) ReflectionTools.getCollectionItemType(interfaceToImplement,propertyName);
	    		((PropertySetter)instance).set(propertyName,
	    				PrimitiveUtils.isPrimitiveClass(collectionItemClass)
	    					? new GeneratePrimitiveCollection<Object>(collectionItemClass,COLLECTION_SIZE).f()
	    					: new GenerateInstanceCollection<Object>(collectionItemClass, COLLECTION_SIZE).f());
	    	}
	    	else 
	    		((PropertySetter)instance).set(propertyName,
	    				PrimitiveUtils.isPrimitiveClass(clazz)
	    					? new GeneratePrimitiveValue<Object>(clazz).f()
	    					: new GenerateInstance<Object>(clazz).f());
	    }
	    return instance;
	}
	
	public interface PropertySetter {
		void set(String property, Object value);
	}
	    
	private static class Interceptor implements MethodInterceptor {
		private PropertyChangeSupport propertySupport;
		   
		public void addPropertyChangeListener(PropertyChangeListener listener) {          
		    propertySupport.addPropertyChangeListener(listener);        
		}
		  
		public void removePropertyChangeListener(PropertyChangeListener listener) {
			propertySupport.removePropertyChangeListener(listener);
		}
		    
		public static Object newInstance( Class clazz ){
			try{
				Interceptor interceptor = new Interceptor();
				Enhancer e = new Enhancer();
				e.setInterfaces(new Class[] {clazz, PropertySetter.class});
				e.setCallback(interceptor);
				Object instance = e.create();
				interceptor.propertySupport = new PropertyChangeSupport( instance );
				return instance;
			}catch( Throwable e ){
				 e.printStackTrace();
				 throw new Error(e.getMessage());
			}
		}
		Dictionary<Object,Object> dictionary = new Hashtable<Object,Object>();
		public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
			Object retValFromSuper = null;
			try {
				if (!Modifier.isAbstract(method.getModifiers())) {
					retValFromSuper = proxy.invokeSuper(obj, args);
				}
			} 
			finally {}
		  
			String name = method.getName();
			if( name.equals("addPropertyChangeListener"))
				addPropertyChangeListener((PropertyChangeListener)args[0]);
			else if ( name.equals( "removePropertyChangeListener" ) )
				removePropertyChangeListener((PropertyChangeListener)args[0]);
	  
			if( name.equals("set")) {
				String propName = (String) args[0];
				dictionary.put(new String( propName ), args[1]);
				propertySupport.firePropertyChange( new String( propName ) , null , args[0]);
			}
			if( name.startsWith("set") && args.length == 1 && method.getReturnType() == Void.TYPE ) {
				char propName[] = name.substring("set".length()).toCharArray();
				propName[0] = Character.toLowerCase( propName[0] );
				dictionary.put(new String( propName ), args[0]);
				propertySupport.firePropertyChange( new String( propName ) , null , args[0]);
			}
			else if( name.startsWith("get") && args.length == 0 ) {
				char propName[] = name.substring("get".length()).toCharArray();
				propName[0] = Character.toLowerCase( propName[0] );
				Object propertyValue = dictionary.get(new String( propName ));
				if (propertyValue != null)
					return propertyValue;
			}
			return retValFromSuper;
		}
	}
}
