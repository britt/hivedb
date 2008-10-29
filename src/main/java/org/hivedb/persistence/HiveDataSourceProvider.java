/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.hivedb.persistence;

import javax.sql.DataSource;
import org.hivedb.Node;

/**
 *
 * @author mellwanger
 */
public interface HiveDataSourceProvider extends DataSourceProvider {
	public DataSource getDataSource(Node node);
}
