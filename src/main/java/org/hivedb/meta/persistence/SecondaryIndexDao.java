/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.hivedb.HiveException;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class SecondaryIndexDao extends JdbcDaoSupport implements
		DataAccessObject<SecondaryIndex, Integer> {
	public SecondaryIndexDao(DataSource ds) {
		this.setDataSource(ds);
	}

	public Integer create(SecondaryIndex newObject) throws SQLException {
		Object[] parameters;
		parameters = new Object[] {
					newObject.getResource().getId(),
					newObject.getColumnInfo().getName(),
					JdbcTypeMapper.jdbcTypeToString(newObject.getColumnInfo()
							.getColumnType()) };
		
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO secondary_index_metadata (resource_id,column_name,db_type) VALUES (?,?,?)",
				new int[] { Types.INTEGER, Types.VARCHAR, Types.VARCHAR });
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new SQLException("Unable to create secondary index: "
					+ parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new SQLException("Unable to retrieve generated primary key");
		newObject.updateId(generatedKey.getKey().intValue());
	
		return new Integer(newObject.getId());
	}

	public List<SecondaryIndex> loadAll() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<SecondaryIndex> results = new ArrayList<SecondaryIndex>();
		for (Object si : t.query("SELECT * FROM SECONDARY_INDEX_METADATA",
				new SecondaryIndexRowMapper())) {
			results.add((SecondaryIndex) si);
		}
		return results;
	}

	class SecondaryIndexRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int index) throws SQLException {
			int jdbcType = Types.OTHER;

			try {
				jdbcType = JdbcTypeMapper.parseJdbcType(rs.getString("db_type"));
			} catch (HiveException ex) {
				throw new SQLException("Unable to discern type: "
						+ rs.getString("db_type") + ", inner message: " + ex.getMessage());
			}

			SecondaryIndex si = new SecondaryIndex(rs.getInt("id"),
					new ColumnInfo(rs.getString("column_name"), jdbcType));
			return si;
		}
	}

	public void update(SecondaryIndex secondaryIndex) throws SQLException {
		Object[] parameters;
		parameters = new Object[] {
					secondaryIndex.getResource().getId(),
					secondaryIndex.getColumnInfo().getName(),
					JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType()),
					secondaryIndex.getId()};
		
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE secondary_index_metadata set resource_id=?,column_name=?,db_type=? where id=?",
				new int[] { Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER });
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new SQLException("Unable to update secondary index: " + secondaryIndex.getId());
	}
	
	public void delete(SecondaryIndex secondaryIndex) throws SQLException {
		Object[] parameters;
		parameters = new Object[] { secondaryIndex.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE from secondary_index_metadata where id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new SQLException("Unable to delete secondary index for id: " + secondaryIndex.getId());
	}

	public List<SecondaryIndex> findByResource(int id) {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<SecondaryIndex> results = new ArrayList<SecondaryIndex>();
		for (Object si : t.query("SELECT * FROM SECONDARY_INDEX_METADATA WHERE RESOURCE_ID = ?",
				new Object[] { id },
				new SecondaryIndexRowMapper())) {
			results.add((SecondaryIndex) si);
		}
		return results;
	}
}
