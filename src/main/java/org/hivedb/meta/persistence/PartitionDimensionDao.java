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
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.util.database.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class PartitionDimensionDao extends JdbcDaoSupport {
	DataSource ds;
	public PartitionDimensionDao(DataSource ds) {
		this.setDataSource(ds);
		this.ds = ds;
	}

	public Integer create(PartitionDimension newObject) {
		
		Object[] parameters;
		parameters = new Object[] { newObject.getName(),
					newObject.getIndexUri(),
					JdbcTypeMapper.jdbcTypeToString(newObject.getColumnType()) };
	
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO partition_dimension_metadata (name,index_uri,db_type) VALUES (?,?,?)",
				new int[] { Types.VARCHAR,Types.VARCHAR,Types.VARCHAR });
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new HiveRuntimeException("Unable to create Partition Dimension: " + parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new HiveRuntimeException("Unable to retrieve generated primary key");
		newObject.updateId(generatedKey.getKey().intValue());
		
		// dependencies
		for (Resource r : newObject.getResources())
			new ResourceDao(ds).create(r);

		return new Integer(newObject.getId());	
	}

	public List<PartitionDimension> loadAll() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<PartitionDimension> results = new ArrayList<PartitionDimension>();
		for (Object result : t.query("SELECT * FROM partition_dimension_metadata",
				new PartitionDimensionRowMapper())) {
			PartitionDimension dimension = (PartitionDimension) result;
			results.add(dimension);
		}
		return results;
	}
	
	@SuppressWarnings("unchecked")
	public PartitionDimension get() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<PartitionDimension> results = 
			(ArrayList<PartitionDimension>) t.query("SELECT * FROM partition_dimension_metadata", new PartitionDimensionRowMapper());
		
		if(results.size() == 0)
			throw new HiveRuntimeException("No PartitionDimension found.");
		else if(results.size() > 1)
			throw new HiveRuntimeException(String.format("Found %s PartitionDImensions, there can be only one.", results.size()));
		PartitionDimension dimension = results.get(0);
		return dimension;
	}
	

	protected class PartitionDimensionRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			final int id = rs.getInt("id");
			List<Resource> resources = new ResourceDao(ds).findByDimension(id);
			PartitionDimension dimension;
				dimension = new PartitionDimension(
						rs.getInt("id"),
						rs.getString("name"),
						JdbcTypeMapper.parseJdbcType(rs.getString("db_type")),
						rs.getString("index_uri"),
						resources);
			
			return dimension;
		}
	}

	public void update(PartitionDimension partitionDimension) {
		
		Object[] parameters;
		parameters = new Object[] { partitionDimension.getName(),
					partitionDimension.getIndexUri(),
					JdbcTypeMapper.jdbcTypeToString(partitionDimension.getColumnType()),
					partitionDimension.getId()};
	
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE partition_dimension_metadata set name=?,index_uri=?,db_type=? where id=?",
				new int[] { Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to update Partition Dimension for id: " + partitionDimension.getId());
	}

	public void delete(PartitionDimension partitionDimension) {
		// dependencies
		for (Resource r : partitionDimension.getResources())
			new ResourceDao(ds).delete(r);
		
		Object[] parameters;
		parameters = new Object[] { partitionDimension.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE from partition_dimension_metadata where id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to delete Partition Dimension for id: " + partitionDimension.getId());	
	}
}
