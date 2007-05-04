package org.hivedb.management;

import org.hivedb.util.database.DerbyTestCase;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHiveInstaller extends DerbyTestCase {
	
	@Test
	public void testHiveInstall() {
		HiveInstaller installer = new HiveInstaller(getConnectString());
		installer.run();
		JdbcDaoSupport dao = new SimpleJdbcDaoSupport();
		dao.setDataSource(getDataSource());
		int rowCount = dao.getJdbcTemplate().queryForInt("select count(1) from semaphore_metadata");
		Assert.assertEquals(1, rowCount);
	}
}
