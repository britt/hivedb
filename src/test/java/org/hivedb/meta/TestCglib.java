package org.hivedb.meta;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Dictionary;
import java.util.Hashtable;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCglib {
	
	@Test
	public void test() {
	    Foo  foo =  (Foo)Interceptor.newInstance( Foo.class );
	    
	    foo.addPropertyChangeListener(
	        new PropertyChangeListener(){
	            public void propertyChange(PropertyChangeEvent evt){
	                System.out.println(evt);
	            }
	        }
	    );
	    try {
			((HiddenPropertySetter)foo).set("sampleProperty","TEST");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	    String sampleProperty = foo.getSampleProperty();
	    Assert.assertEquals(sampleProperty,"TEST");
	}   
	public interface HiddenPropertySetter {
		void set(String property, Object value);
	}
	public interface Foo extends java.io.Serializable{    
	
	     public void addPropertyChangeListener(PropertyChangeListener listener); 
	     public void removePropertyChangeListener(PropertyChangeListener listener); 
	     public String getSampleProperty();
	     public String toString();
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
				e.setInterfaces(new Class[] {clazz, HiddenPropertySetter.class});
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
