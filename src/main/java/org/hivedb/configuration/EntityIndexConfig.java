package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.hibernate.annotations.IndexType;
import org.hivedb.util.functional.Validator;

public interface EntityIndexConfig
{
	Collection<Object> getIndexValues(Object entityInstance);	
	String getIndexName();
	String getPropertyName();
	Class<?> getIndexClass();
	IndexType getIndexType();
	Validator getValidator();
}