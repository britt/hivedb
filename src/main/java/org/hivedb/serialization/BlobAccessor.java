package org.hivedb.serialization;

import org.hibernate.PropertyNotFoundException;
import org.hibernate.property.Getter;
import org.hibernate.property.PropertyAccessor;
import org.hibernate.property.Setter;

public class BlobAccessor implements PropertyAccessor {

	public Getter getGetter(Class clazz, String propertyName) throws PropertyNotFoundException {
		return new BlobGetter();
	}

	public Setter getSetter(Class clazz, String propertyName) throws PropertyNotFoundException {
		return new BlobSetter();
	}

}
