package org.hivedb.configuration.entity;

import org.hivedb.annotations.IndexType;
import org.hivedb.util.validators.Validator;

import java.util.Collection;

public interface EntityIndexConfig {
	Collection<Object> getIndexValues(Object entityInstance);	
	String getIndexName();
	String getInnerClassPropertyName(); // Only valid for collection properties
	String getPropertyName();
	Class<?> getIndexClass();
	IndexType getIndexType();
	Validator getValidator();
}