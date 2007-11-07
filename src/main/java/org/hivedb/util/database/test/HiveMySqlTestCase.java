package org.hivedb.util.database.test;

import java.sql.Types;
import java.util.ArrayList;

import org.hivedb.Hive;
import org.hivedb.management.HiveInstaller;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class HiveMySqlTestCase extends MysqlTestCase {
	@Override
	@BeforeClass
	public void beforeClass() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		if(this.getDatabaseNames() == null)
			setDatabaseNames(new ArrayList<String>());
		if(!getDatabaseNames().contains(getHiveDatabaseName())) {
			ArrayList<String> names = new ArrayList<String>();
			names.addAll(getDatabaseNames());
			names.add(getHiveDatabaseName());
			setDatabaseNames(names);
		}
		super.beforeClass();
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();
	}
	
	public abstract String getHiveDatabaseName();
	
	protected Hive createHive(String dimensionName) {
		return Hive.create(
				getConnectString(getHiveDatabaseName()),
				dimensionName,
				Types.INTEGER);
	}
}
