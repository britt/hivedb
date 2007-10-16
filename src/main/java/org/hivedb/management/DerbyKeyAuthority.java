package org.hivedb.management;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;


/**
 * In Membory key authority for Derby
 * @param <T> Type of generated keys (Long or Integer)
 */
public class DerbyKeyAuthority extends JdbcKeyAuthority {
	public DerbyKeyAuthority(Class keySpace, Class returnType) {
		super(keySpace, returnType);
		setIncrementer(this.getIncrementer(null));
	}

	private DataFieldMaxValueIncrementer getIncrementer(DataSource ds) {
		return new DerbyDataFieldMaxValueIncrementer();
	}
	
	private static class DerbyDataFieldMaxValueIncrementer implements DataFieldMaxValueIncrementer {
		static int nextInt = 0;
		static long nextLong = 0;
		public int nextIntValue() throws DataAccessException {
			return ++nextInt;
		}

		public long nextLongValue() throws DataAccessException {				
			return ++nextLong;
		}

		public String nextStringValue() throws DataAccessException {
			throw new RuntimeException("Strings are not supported");
		}
		
	};
	protected void createSchema() {}
}
