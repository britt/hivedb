/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.hivedb.meta.persistence;

import org.hivedb.meta.Node;

import javax.sql.DataSource;

/**
 * @author mellwanger
 */
public interface HiveDataSourceProvider extends DataSourceProvider {
  public DataSource getDataSource(Node node);

  /**
   * Should be called when done with the provider to release underlying resources (e.g. connections)
   */
  public void close();
}
