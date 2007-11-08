package org.hivedb;

import org.hivedb.configuration.HiveConfigurationSchema;
import org.testng.annotations.Test;

public class TestMetadataSchemaWithMysql {
	@Test(groups={"mysql"})
	public void testInstallMySql() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		String connectString = "jdbc:mysql://localhost/test?user=test&password=test";
		HiveConfigurationSchema schema = new HiveConfigurationSchema(connectString);
		schema.install();
	}
}
