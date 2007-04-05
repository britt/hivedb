package org.hivedb;

import org.hivedb.meta.GlobalSchema;

public class TestMetadataSchemaWithMysql {
//	@Test(groups={"mysql"})
	public void testInstallMySql() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		String connectString = "jdbc:mysql://localhost/test?user=test&password=test";
		GlobalSchema schema = new GlobalSchema(connectString);
		schema.install();
	}
}
