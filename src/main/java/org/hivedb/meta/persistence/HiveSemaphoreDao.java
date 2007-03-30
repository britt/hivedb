/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.hivedb.meta.HiveSemaphore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class HiveSemaphoreDao extends JdbcDaoSupport {
	public HiveSemaphoreDao(DataSource ds) {
		this.setDataSource(ds);
	}

	public HiveSemaphore get() throws SQLException {
		try {
			return getSemaphore();
		} catch (Exception ex) {
			create();
			return getSemaphore();			
		}
	}

	private HiveSemaphore getSemaphore() throws SQLException {
		JdbcTemplate t = getJdbcTemplate();
		HiveSemaphore result;
		try {
			result = (HiveSemaphore) t.queryForObject(
					"SELECT * FROM semaphore_metadata", 
					new HiveSemaphoreRowMapper());
		} catch (Exception ex) {
			throw new SQLException(
					"Exception loading HiveSemaphore -- verify that semaphore_metadata has one and only one row: "
							+ ex.getMessage());
		}
		return result;
	}

	public HiveSemaphore create() throws SQLException {
		try {
			// check to see if it already exists
			return getSemaphore();
		} catch (SQLException e) {
			JdbcTemplate j = getJdbcTemplate();
			Object[] parameters = new Object[] { 0, 1 };
			int rows = j
					.update(
							"INSERT INTO semaphore_metadata (read_only,revision) VALUES (?,?)",
							parameters);
			if (rows != 1)
				throw new SQLException("Unable to create HiveSemaphore: "
						+ parameters);
			return get();
		}
	}

	/***************************************************************************
	 * Update HiveSemaphore. Will perform a single attempt to create the
	 * semaphore if any Exception is encountered.
	 * 
	 * @param hs
	 * @throws SQLException
	 */
	public void update(HiveSemaphore hs) throws SQLException {
		try {
			doUpdate(hs);
		} catch (Exception ex) {
			create();
			// avoid recursing by calling another method
			doUpdate(hs);
		}
	}

	private void doUpdate(HiveSemaphore hs) {
		Object[] parameters = new Object[] { hs.isReadOnly() ? 1 : 0,
				hs.getRevision() };
		JdbcTemplate j = getJdbcTemplate();
		j.update("UPDATE semaphore_metadata SET read_only = ?, revision = ?",
				parameters);
	}

	protected class HiveSemaphoreRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return new HiveSemaphore(
					rs.getInt("read_only") == 0 ? false : true, rs
							.getInt("revision"));
		}
	}

	public void incrementAndPersist() throws SQLException {
		HiveSemaphore hs = get();
		hs.incrementRevision();
		update(hs);
	}
}
