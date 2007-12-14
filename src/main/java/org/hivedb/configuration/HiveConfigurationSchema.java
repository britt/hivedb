/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.configuration;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.Templater;

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
public class HiveConfigurationSchema extends Schema {	
	/**
	 * GlobalSchema is constructed against a JDBC URI, which will be the destination
	 * for the schema tables.
	 * 
	 * @param dbURI Empty target database connect string, including username, password & catalog
	 */
	public HiveConfigurationSchema(String dbURI){
		super("Hive configuration schema",dbURI);
	}
	
	private String getCreateNode() {
		return Templater.render("sql/node_configuration.vsql", getContext());
	}

	private String getCreateHive() {
		return Templater.render("sql/hive_semaphore.vsql", getContext());
	}
	
	private String getCreatePartitionDimension() {
		return Templater.render("sql/partition_dimension_configuration.vsql", getContext());
	}

	private String getCreateSecondaryIndex() {
		return Templater.render("sql/secondary_index_configuration.vsql", getContext());
	}
	
	private String getCreateResource() {
		return Templater.render("sql/resource_configuration.vsql", getContext());
	}
	
	public void install() {
		super.install();
		BasicDataSource ds = new BasicDataSource();
		ds.setUrl(this.uri);
	}

	public String[] getCreateStatements() {
		return new String[] {
				getCreateHive(),
				getCreateNode(),
				getCreatePartitionDimension(),
				getCreateSecondaryIndex(),
				getCreateResource()};
	}

	@Override
	public Collection<TableInfo> getTables() {
		Collection<TableInfo> TableInfos = new ArrayList<TableInfo>();
		TableInfos.add(new TableInfo("semaphore_metadata", getCreateHive()));
		TableInfos.add(new TableInfo("node_metadata", getCreateNode()));
		TableInfos.add(new TableInfo("partition_dimension_metadata", getCreatePartitionDimension()));
		TableInfos.add(new TableInfo("secondary_index_metadata", getCreateSecondaryIndex()));
		TableInfos.add(new TableInfo("resource_metadata", getCreateResource()));
		return TableInfos;
	}
}
