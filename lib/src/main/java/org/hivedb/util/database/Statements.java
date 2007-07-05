package org.hivedb.util.database;

import org.springframework.jdbc.core.PreparedStatementCreatorFactory;

public class Statements {
	/***
	 * Taking advantage of variable length argument lists to shorten the syntax
	 * @param sql
	 * @param types
	 * @return
	 */
	public static PreparedStatementCreatorFactory newStmtCreatorFactory(String sql, int... types) {
		return new PreparedStatementCreatorFactory(sql, types);
	}
}
