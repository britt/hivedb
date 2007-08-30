package org.hivedb.management;


import org.hivedb.management.KeyAuthority;

public interface KeyAuthorityCreator {
	<T extends Number> KeyAuthority<T> create(Class keySpace, Class<T> returnType);
}
