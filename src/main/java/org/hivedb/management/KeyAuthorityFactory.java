package org.hivedb.management;

import org.hivedb.util.database.HiveDbDialect;

public class KeyAuthorityFactory {
	public static KeyAuthority create(
			HiveDbDialect dialect,
			Class keySpace, 
			Class<?> returnType) {
		if (dialect.equals(HiveDbDialect.MySql))
			return new JdbcKeyAuthority(keySpace, returnType);
		if (dialect.equals(HiveDbDialect.H2))
			return new MemoryKeyAuthority().create(keySpace, returnType);
		throw new RuntimeException(String.format("Unknown HiveDbDialect %s", dialect.name()));
	}
}
