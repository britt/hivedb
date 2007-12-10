package org.hivedb.hibernate;

import java.io.Serializable;

public interface DataAccessObjectFactory<T, ID extends Serializable> {
	DataAccessObject<T, ID> create();
}
