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
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
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
public class PartitionDimensionDao extends JdbcDaoSupport implements DataAccessObject<PartitionDimension,Integer> {
	DataSource ds;
	public PartitionDimensionDao(DataSource ds) {
		this.setDataSource(ds);
		this.ds = ds;
	}

	public Integer create(PartitionDimension newObject) {
		// dependencies
		if (newObject.getNodeGroup().getId()==0)
			new NodeGroupDao(ds).create(newObject.getNodeGroup());
		
		Object[] parameters;
		parameters = new Object[] { newObject.getName(),
					newObject.getNodeGroup().getId(),
					newObject.getIndexUri(),
					JdbcTypeMapper.jdbcTypeToString(newObject.getColumnType()) };
	
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO partition_dimension_metadata (name,node_group_id,index_uri,db_type) VALUES (?,?,?,?)",
				new int[] { Types.VARCHAR,Types.INTEGER,Types.VARCHAR,Types.VARCHAR });
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
			results.add((PartitionDimension) result);
		}
		return results;
	}
	

	protected class PartitionDimensionRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			final int id = rs.getInt("id");
			List<Resource> resources = new ResourceDao(ds).findByDimension(id);
			NodeGroup dataNodes = new NodeGroupDao(ds).get(rs.getInt("node_group_id"));
			
			PartitionDimension dimension;
				dimension = new PartitionDimension(
						rs.getInt("id"),
						rs.getString("name"),
						JdbcTypeMapper.parseJdbcType(rs.getString("db_type")),
						dataNodes, 
						rs.getString("index_uri"),
						resources);
			
			return dimension;
		}
	}

	public void update(PartitionDimension partitionDimension) {
		
		Object[] parameters;
		parameters = new Object[] { partitionDimension.getName(),
					partitionDimension.getNodeGroup().getId(),
					partitionDimension.getIndexUri(),
					JdbcTypeMapper.jdbcTypeToString(partitionDimension.getColumnType()),
					partitionDimension.getId()};
	
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE partition_dimension_metadata set name=?,node_group_id=?,index_uri=?,db_type=? where id=?",
				new int[] { Types.VARCHAR,Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to update Partition Dimension for id: " + partitionDimension.getId());
	}

	public void delete(PartitionDimension partitionDimension) {
		// dependencies
		new NodeGroupDao(ds).delete(partitionDimension.getNodeGroup());
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
