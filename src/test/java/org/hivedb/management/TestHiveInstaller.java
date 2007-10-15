package org.hivedb.management;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.util.database.test.H2TestCase;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHiveInstaller extends H2TestCase {
	private static String TEST_DB = "testDB";
	@Test
	public void testHiveInstall() {
		HiveInstaller installer = new HiveInstaller(getConnectString(TEST_DB));
		installer.run();
		JdbcDaoSupport dao = new SimpleJdbcDaoSupport();
		dao.setDataSource(getDataSource(TEST_DB));
		int rowCount = dao.getJdbcTemplate().queryForInt("select count(1) from semaphore_metadata");
		Assert.assertEquals(1, rowCount);
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{TEST_DB});
	}
}
