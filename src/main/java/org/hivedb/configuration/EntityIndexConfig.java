package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.configuration.EntityConfig.IndexType;
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