package org.hivedb.hibernate;

import java.lang.reflect.Method;

import org.hibernate.HibernateException;
import org.hibernate.PropertyNotFoundException;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.property.BasicPropertyAccessor;
import org.hibernate.property.Setter;
import org.hivedb.util.GeneratedInstanceInterceptor;

public class GeneratedAccessor extends BasicPropertyAccessor {
	
	@Override
	public Setter getSetter(Class theClass, String propertyName) throws PropertyNotFoundException {
		return new GeneratedInstanceSetter(propertyName);
	}
	public static class GeneratedInstanceSetter implements Setter {
		
		private static final long serialVersionUID = 1L;
		private String propertyName;
		public GeneratedInstanceSetter(String propertyName) {
			this.propertyName = propertyName;
		}
		public Method getMethod() {
			// optional method @see hibernate docs
			return null;
		}

		public String getMethodName() {
			// optional method @see hibernate docs
			return null;
		}

		@SuppressWarnings("unchecked")
		public void set(Object target, Object value, SessionFactoryImplementor sessionFactory) throws HibernateException {
			GeneratedInstanceInterceptor.setProperty(target, propertyName, value);	
		}
	}
}
