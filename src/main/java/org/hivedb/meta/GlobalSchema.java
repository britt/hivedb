/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.HiveException;
import org.hivedb.Schema;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

/**
 * The Global Hive Configuration schema contains records of the Hive's internal
 * partitioning, indexing, and node allocation.
 * <p>
 * Generic DDL is used for portability. No indexing is performed, as global schema 
 * records will number in the hundreds or thousands, and all records are loaded 
 * upon each access.
 * </p>
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class GlobalSchema extends Schema {	
	/**
	 * GlobalSchema is constructed against a JDBC URI, which will be the destination
	 * for the schema tables.
	 * 
	 * @param dbURI Empty target database connect string, including username, password & catalog
	 */
	public GlobalSchema(String dbURI) throws HiveException {
		super(dbURI);
	}
	
	private String getCreateNode() {
		return "CREATE TABLE node_metadata ( "
				+ "id " + getNumericPrimaryKeySequenceModifier(dialect) + ", " 
				+ "node_group_id int not null, "
				+ "uri varchar(255) not null, "
				+ "read_only int " // Derby doesn't have BIT
				+ " )";
	}

	private String getCreateNodeGroup() {
		return "CREATE TABLE node_group_metadata ( " 
				+ "id " + getNumericPrimaryKeySequenceModifier(dialect) 
				+ " )";
	}

	private String getCreateHive() {
		return "CREATE TABLE semaphore_metadata ( " 
				+ "read_only int not null," 
				+ "revision int not null"
				+ " )";
	}
	
	private String getCreatePartitionDimension() {
		return "CREATE TABLE partition_dimension_metadata ( " 
				+ "id " + getNumericPrimaryKeySequenceModifier(dialect) + ", " 
				+ "node_group_id int not null, " 
				+ "name varchar(64) not null, " 
				+ "index_uri varchar(255) not null, "
				+ "db_type varchar(64) not null" 
				+ " )";
	}

	private String getCreateSecondaryIndex() {
		return "CREATE TABLE secondary_index_metadata ( " 
				+ "id " + getNumericPrimaryKeySequenceModifier(dialect) + ", " 
				+ "resource_id int not null, " 
				+ "column_name varchar(64) not null, "
				+ "db_type varchar(64) not null"
				+ " )";
	}
	
	private String getCreateResource() {
		return "CREATE TABLE resource_metadata ( " 
				+ "id " + getNumericPrimaryKeySequenceModifier(dialect) + ", " 
				+ "dimension_id int not null, "
				+ "name varchar(128) not null "
				+ " )";
	}
	
	public void install() throws SQLException {
		super.install();
		BasicDataSource ds = new BasicDataSource();
		ds.setUrl(this.dbURI);
		new HiveSemaphoreDao(ds).create();
	}

	public String[] getCreateStatements() {
		return new String[] {
				getCreateHive(),
				getCreateNode(),
				getCreateNodeGroup(),
				getCreatePartitionDimension(),
				getCreateSecondaryIndex(),
				getCreateResource()};
	}

	@Override
	public Collection<TableInfo> getTables() {
		Collection<TableInfo> TableInfos = new ArrayList<TableInfo>();
		TableInfos.add(new TableInfo("semaphore_metadata", getCreateHive()));
		TableInfos.add(new TableInfo("node_metadata", getCreateNode()));
		TableInfos.add(new TableInfo("node_group_metadata", getCreateNodeGroup()));
		TableInfos.add(new TableInfo("partition_dimension_metadata", getCreatePartitionDimension()));
		TableInfos.add(new TableInfo("secondary_index_metadata", getCreateSecondaryIndex()));
		TableInfos.add(new TableInfo("resource_metadata", getCreateResource()));
		return TableInfos;
	}
}
