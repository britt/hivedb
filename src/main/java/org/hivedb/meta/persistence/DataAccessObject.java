/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

/**
 * Data Access Object interface suitable for our Hive domain objects
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 *
 * @param <T>	Domain object type for which this DAO responsible
 * @param <PK>	Type of key
 */
public interface DataAccessObject <T, PK extends Serializable> {
    PK create(T newObject) throws SQLException;
    void update(T object) throws SQLException;
    List<T> loadAll() throws SQLException;
}