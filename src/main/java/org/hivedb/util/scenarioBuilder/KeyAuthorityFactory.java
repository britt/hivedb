package org.hivedb.util.scenarioBuilder;

import javax.sql.DataSource;

import org.hivedb.management.KeyAuthority;
import org.hivedb.management.MySqlKeyAuthority;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public interface KeyAuthorityFactory {
	<T extends Number> KeyAuthority<T> create(Class keySpace, Class<T> returnType);

	public static class Memory implements KeyAuthorityFactory {
		
		@SuppressWarnings("unchecked")
		public <T extends Number> KeyAuthority<T> create(Class keySpace, final Class<T> returnType) {
			final Generator<T> incrementor;
			if (returnType.equals(int.class) || returnType.equals(Integer.class))
				 incrementor = (Generator<T>) new Generator<Integer>() {
					private int i=0;;
					public Integer f() {
						return ++i;
					}
				};
			else if (returnType.equals(long.class) || returnType.equals(Long.class))
				incrementor = (Generator<T>) new Generator<Long>() {
					private long i=0;;
					public Long f() {
						return ++i;
					}
				};
			else
				throw new RuntimeException("Only Integers and Longs are supported");
			
			return new KeyAuthority<T>() {

				public T nextAvailableKey() {
					return increment();
				}
				private T increment()
				{
					return incrementor.f();
				}
			};
		}
	}
	
	public static class Mysql implements KeyAuthorityFactory {
		
		private String connectStringWithoutDatabase, connectStringWithDatabase, databaseName;
		private DataSource datasource;
		public Mysql(String connectStringWithoutDatabase, String connectStringWithDatabase, String databaseName, DataSource datasource)
		{
			this.connectStringWithoutDatabase = connectStringWithoutDatabase;
			this.connectStringWithDatabase = connectStringWithDatabase;
			this.databaseName = databaseName;
			this.datasource = datasource;
		}
		public <T extends Number> KeyAuthority<T> create(Class keySpace, Class<T> returnType) {
			return new MySqlKeyAuthority<T>(datasource, keySpace ,returnType);
		}
		
		public DataSource getDataSource() {
			DataSource datasource = new HiveBasicDataSource(getConnectStringWithoutDatabase());
			try {
				datasource.getConnection().createStatement().execute(
						"DROP DATABASE " + getDatabaseName());
			} catch (Exception ex) { // ok
			}
			try {
				datasource.getConnection().createStatement().execute(
					"CREATE DATABASE " + getDatabaseName());
				datasource = new HiveBasicDataSource(getConnectString());
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			return datasource;
		}
		
		private String getConnectStringWithoutDatabase() {
			return connectStringWithoutDatabase;
		}
	
		private String getConnectString() {
			return connectStringWithDatabase;
		}
	
		private String getDatabaseName() {
			return databaseName;
		}
	}
}
