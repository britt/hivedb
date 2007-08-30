package org.hivedb.util.database;

import java.util.ArrayList;

import org.hivedb.management.HiveInstaller;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class HiveMySqlTestCase extends MysqlTestCase {
	@Override
	@BeforeClass
	protected void beforeClass() {
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
}
