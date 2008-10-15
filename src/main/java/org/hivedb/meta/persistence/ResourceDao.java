/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceImpl;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class ResourceDao extends JdbcDaoSupport {
	private DataSource ds;

	public ResourceDao(DataSource ds) {
		this.ds = ds;
		this.setDataSource(ds);
	}

	public List<ResourceImpl> loadAll() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<ResourceImpl> results = new ArrayList<ResourceImpl>();
		for (Object si : t.query("SELECT * FROM resource_metadata",
				new ResourceRowMapper())) {
			
			results.add((ResourceImpl)si);
		}
		return results;
	}

	public Integer create(Resource newResource) {
		int columnType = newResource.getIdIndex().getColumnInfo().getColumnType();
		Object[] parameters = new Object[] { newResource.getName(), newResource.getPartitionDimension().getId(), JdbcTypeMapper.jdbcTypeToString(columnType),newResource.isPartitioningResource()};
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO resource_metadata (name,dimension_id,db_type,is_partitioning_resource) VALUES (?,?,?,?)",
				new int[] {Types.VARCHAR,Types.INTEGER,Types.VARCHAR,Types.BIT});
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new HiveRuntimeException("Unable to create Resource: "
					+ parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new HiveRuntimeException("Unable to retrieve generated primary key");
		newResource.setId(generatedKey.getKey().intValue());
		
		// dependencies
		for (SecondaryIndex si : newResource.getSecondaryIndexes())
			new SecondaryIndexDao(ds).create(si);
			
		return new Integer(newResource.getId());
	}

	public void update(Resource resource) {
		int columnType = resource.getIdIndex().getColumnInfo().getColumnType();
		Object[] parameters = new Object[] { resource.getName(), resource.getPartitionDimension().getId(), JdbcTypeMapper.jdbcTypeToString(columnType),resource.isPartitioningResource(),resource.getId()};
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE resource_metadata SET name=?,dimension_id=?,db_type=?,is_partitioning_resource=? WHERE id=?",
				new int[] {Types.VARCHAR,Types.INTEGER,Types.VARCHAR,Types.INTEGER,Types.BIT});
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new HiveRuntimeException("Unable to update Resource: " + resource.getId());
		
		// dependencies
		for (SecondaryIndex si : resource.getSecondaryIndexes())
			new SecondaryIndexDao(ds).update(si);
	}
	
	public void delete(Resource resource) {
		// dependencies
		for (SecondaryIndex si : resource.getSecondaryIndexes())
			new SecondaryIndexDao(ds).delete(si);
		
		Object[] parameters;
		parameters = new Object[] { resource.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE FROM resource_metadata WHERE id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to delete resource for id: " + resource.getId());
	}

	protected class ResourceRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			SecondaryIndexDao sDao = new SecondaryIndexDao(ds);
			List<SecondaryIndex> indexes = sDao.findByResource(rs.getInt("id"));
			return new ResourceImpl(rs.getInt("id"), rs.getString("name"), JdbcTypeMapper.parseJdbcType(rs.getString("db_type")), rs.getBoolean("is_partitioning_resource"), indexes);
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
