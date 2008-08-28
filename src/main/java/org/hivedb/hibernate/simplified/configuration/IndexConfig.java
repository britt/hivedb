package org.hivedb.hibernate.simplified.configuration;

import org.hivedb.annotations.IndexType;
import org.hivedb.util.validators.Validator;

public interface IndexConfig {
	String getIndexName();
	String getPropertyName();
	Class<?> getIndexClass();
	IndexType getIndexType();
	Validator getValidator();
}

