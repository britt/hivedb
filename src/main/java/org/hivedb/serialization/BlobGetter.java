package org.hivedb.serialization;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import org.hibernate.HibernateException;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.property.Getter;
import org.hivedb.util.Converters;


public class BlobGetter implements Getter {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7192532599957992415L;

	public Object get(Object owner) throws HibernateException {
		InputStream stream = XmlXStreamSerializationProvider.instance().getSerializer(owner.getClass()).serialize(owner);
		SerialBlob blob;
		try {
			blob = new SerialBlob(Converters.getBytes(stream));
		} catch (SerialException e) {
			throw new HibernateException(e);
		} catch (SQLException e) {
			throw new HibernateException(e);
		}
		return blob;
	}

	@SuppressWarnings("unchecked")
	public Object getForInsert(Object owner, Map mergeMap, SessionImplementor session) throws HibernateException {
		return get(owner);
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
	public Class getReturnType() {
		return Blob.class;
	}
	
}
