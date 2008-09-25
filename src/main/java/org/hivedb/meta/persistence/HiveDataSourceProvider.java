/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.hivedb.meta.persistence;

import javax.sql.DataSource;
import org.hivedb.meta.Node;

/**
 *
 * @author mellwanger
 */
public interface HiveDataSourceProvider extends DataSourceProvider {
	public DataSource getDataSource(Node node);
}
