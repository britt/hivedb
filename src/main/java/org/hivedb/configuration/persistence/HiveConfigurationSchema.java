/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.configuration.persistence;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.persistence.Schema;
import org.hivedb.persistence.TableInfo;
import org.hivedb.util.Templater;
import org.hivedb.util.database.Schemas;

import java.util.ArrayList;
import java.util.Collection;

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
  private String dbURI;

  /**
   * GlobalSchema is constructed against a JDBC URI, which will be the destination
   * for the schema tables.
   *
   * @param dbURI Empty target database connect string, including username, password & catalog
   */
  public HiveConfigurationSchema(String dbURI) {
    this();
    this.dbURI = dbURI;
  }

  public HiveConfigurationSchema() {
    super("Hive configuration schema");
  }

  private String getCreateNode() {
    return Templater.render("sql/node_configuration.vsql", Schemas.getContext(dbURI));
  }

  private String getCreateHive() {
    return Templater.render("sql/hive_semaphore.vsql", Schemas.getContext(dbURI));
  }

  private String getCreatePartitionDimension() {
    return Templater.render("sql/partition_dimension_configuration.vsql", Schemas.getContext(dbURI));
  }

  private String getCreateSecondaryIndex() {
    return Templater.render("sql/secondary_index_configuration.vsql", Schemas.getContext(dbURI));
  }

  private String getCreateResource() {
    return Templater.render("sql/resource_configuration.vsql", Schemas.getContext(dbURI));
  }

  public void install() {
    Schemas.install(this, dbURI);
    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(dbURI);
  }

  public String[] getCreateStatements() {
    return new String[]{
      getCreateHive(),
      getCreateNode(),
      getCreatePartitionDimension(),
      getCreateSecondaryIndex(),
      getCreateResource()};
  }

  @Override
  public Collection<TableInfo> getTables(String uri) {
    Collection<TableInfo> TableInfos = new ArrayList<TableInfo>();
    TableInfos.add(new TableInfo("semaphore_metadata", getCreateHive()));
    TableInfos.add(new TableInfo("node_metadata", getCreateNode()));
    TableInfos.add(new TableInfo("partition_dimension_metadata", getCreatePartitionDimension()));
    TableInfos.add(new TableInfo("secondary_index_metadata", getCreateSecondaryIndex()));
    TableInfos.add(new TableInfo("resource_metadata", getCreateResource()));
    return TableInfos;
  }
}
