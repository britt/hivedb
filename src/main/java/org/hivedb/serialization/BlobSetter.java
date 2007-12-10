package org.hivedb.serialization;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Collection;

import org.hibernate.HibernateException;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.property.Setter;
import org.hivedb.util.PropertySetter;
import org.hivedb.util.ReflectionTools;
import org.springframework.beans.BeanUtils;

public class BlobSetter implements Setter {
	private static final long serialVersionUID = 1;

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
		InputStream stream;
		try {
			stream = ((Blob) value).getBinaryStream();
		} catch (SQLException e) {
			throw new HibernateException(e);
		}
		Object defrosted = XmlXStreamSerializationProvider.instance().getSerializer(target.getClass()).deserialize(stream);
		Class<?> clazz = 
			ReflectionTools.whichIsImplemented(
					(Class)defrosted.getClass(), 
					(Collection<Class>)XmlXStreamSerializationProvider.instance().getSerializableInterfaces());
		for(Method get : ReflectionTools.getGetters(clazz)) {
			Object propertyValue;
			try {
				propertyValue = get.invoke(defrosted, new Object[]{});
			} catch (IllegalArgumentException e) {
				throw new HibernateException(e);
			} catch (IllegalAccessException e) {
				throw new HibernateException(e);
			} catch (InvocationTargetException e) {
				throw new HibernateException(e);
			}
			
			ReflectionTools.invokeSetter(target, BeanUtils.findPropertyForMethod(get).getName(), propertyValue);
		}
	}

}
