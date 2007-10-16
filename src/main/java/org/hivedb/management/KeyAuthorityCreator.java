package org.hivedb.management;


import org.hivedb.management.KeyAuthority;

public interface KeyAuthorityCreator {
	KeyAuthority create(Class keySpace, Class returnType);
}
