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

  public void close();
}
