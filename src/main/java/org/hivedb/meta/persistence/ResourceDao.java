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

import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class ResourceDao extends JdbcDaoSupport implements
		DataAccessObject<Resource, Integer> {
	private DataSource ds;

	public ResourceDao(DataSource ds) {
		this.ds = ds;
		this.setDataSource(ds);
	}

	public List<Resource> loadAll() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<Resource> results = new ArrayList<Resource>();
		for (Object si : t.query("SELECT * FROM resource_metadata",
				new ResourceRowMapper())) {
			results.add((Resource) si);
		}
		return results;
	}

	public Integer create(Resource newObject) throws SQLException {
		Object[] parameters = new Object[] { newObject.getName(), newObject.getPartitionDimension().getId()};
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO resource_metadata (name,dimension_id) VALUES (?,?)",
				new int[] {Types.VARCHAR,Types.INTEGER});
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new SQLException("Unable to create Resource: "
					+ parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new SQLException("Unable to retrieve generated primary key");
		newObject.updateId(generatedKey.getKey().intValue());
		
		// dependencies
		for (SecondaryIndex si : newObject.getSecondaryIndexes())
			new SecondaryIndexDao(ds).create(si);
			
		return new Integer(newObject.getId());
	}

	public void update(Resource resource) throws SQLException {
		Object[] parameters = new Object[] { resource.getName(), resource.getPartitionDimension().getId(), resource.getId()};
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE resource_metadata SET name=?,dimension_id=? WHERE id=?",
				new int[] {Types.VARCHAR,Types.INTEGER,Types.INTEGER});
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new SQLException("Unable to update Resource: " + resource.getId());
	}
	
	public void delete(Resource resource) throws SQLException {
		// dependencies
		for (SecondaryIndex si : resource.getSecondaryIndexes())
			new SecondaryIndexDao(ds).create(si);
		
		Object[] parameters;
		parameters = new Object[] { resource.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE FROM resource_metadata WHERE id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new SQLException("Unable to delete resource for id: " + resource.getId());
	}

	protected class ResourceRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			SecondaryIndexDao sDao = new SecondaryIndexDao(ds);
			List<SecondaryIndex> indexes = sDao.findByResource(rs.getInt("id"));
			return new Resource(rs.getInt("id"), rs.getString("name"), indexes);
		}
	}

	public List<Resource> findByDimension(int id) {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<Resource> results = new ArrayList<Resource>();
		for (Object r : t.query("SELECT * FROM resource_metadata WHERE dimension_id = ?",
				new Object[] { id },
				new ResourceRowMapper())) {
			results.add((Resource) r);
		}
		return results;
	}
}
