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

import org.hivedb.HiveRuntimeException;
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

	public Integer create(SecondaryIndex newObject) {
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
			throw new HiveRuntimeException("Unable to create secondary index: "
					+ parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new HiveRuntimeException("Unable to retrieve generated primary key");
		newObject.updateId(generatedKey.getKey().intValue());
	
		return new Integer(newObject.getId());
	}

	public List<SecondaryIndex> loadAll() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<SecondaryIndex> results = new ArrayList<SecondaryIndex>();
		for (Object si : t.query("SELECT * FROM secondary_index_metadata",
				new SecondaryIndexRowMapper())) {
			results.add((SecondaryIndex) si);
		}
		return results;
	}

	class SecondaryIndexRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int index) throws SQLException{
			int jdbcType = Types.OTHER;
			jdbcType = JdbcTypeMapper.parseJdbcType(rs.getString("db_type"));
			
			SecondaryIndex si = new SecondaryIndex(rs.getInt("id"),
					new ColumnInfo(rs.getString("column_name"), jdbcType));
			return si;
		}
	}

	public void update(SecondaryIndex secondaryIndex) {
		Object[] parameters;
		parameters = new Object[] {
					secondaryIndex.getResource().getId(),
					secondaryIndex.getColumnInfo().getName(),
					JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType()),
					secondaryIndex.getId()};
		
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE secondary_index_metadata SET resource_id=?,column_name=?,db_type=? WHERE id=?",
				new int[] { Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER });
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new HiveRuntimeException("Unable to update secondary index: " + secondaryIndex.getId());
	}
	
	public void delete(SecondaryIndex secondaryIndex) {
		Object[] parameters;
		parameters = new Object[] { secondaryIndex.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE FROM secondary_index_metadata WHERE id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to delete secondary index for id: " + secondaryIndex.getId());
	}

	public List<SecondaryIndex> findByResource(int id) {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<SecondaryIndex> results = new ArrayList<SecondaryIndex>();
		for (Object si : t.query("SELECT * FROM secondary_index_metadata WHERE resource_id = ?",
				new Object[] { id },
				new SecondaryIndexRowMapper())) {
			results.add((SecondaryIndex) si);
		}
		return results;
	}
}
