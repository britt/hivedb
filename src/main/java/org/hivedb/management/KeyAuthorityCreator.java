package org.hivedb.management;

public interface KeyAuthorityCreator {
	KeyAuthority create(Class keySpace, Class returnType);
}
