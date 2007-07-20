package org.hivedb.management.statistics;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import javax.sql.DataSource;

import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

// TODO Lots of messy SQL in these DAOs

/**
 * 
 * @author Britt Crawford (bcrawford@cafepress.com)
 *
 */
public class PartitionKeyStatisticsDao extends JdbcDaoSupport {
	public PartitionKeyStatisticsDao(DataSource ds) {
		this.setDataSource(ds);
	}
	
	public PartitionKeyStatisticsBean findByPrimaryPartitionKey(PartitionDimension dimension, Object key){
		PartitionKeyStatisticsBean stats = null;
		try {
			stats = 
				(PartitionKeyStatisticsBean) getJdbcTemplate().queryForObject(
						selectSql(IndexSchema.getPrimaryIndexTableName(dimension)),
						new Object[] {key}, 
						new PartitionKeyStatisticsRowMapper(dimension));
		} catch( EmptyResultDataAccessException e) {
			throw new HiveKeyNotFoundException(String.format("PartitionKeyStatistics not found for key %s", key),key);
		}
		return stats;		
	}

	@SuppressWarnings("unchecked")
	public List<PartitionKeyStatistics> findAllByNodeAndDimension(PartitionDimension dimension, Node node) {
		Object[] parameters = new Object[] {node.getId()};
		
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(
				selectByNodeSql(IndexSchema.getPrimaryIndexTableName(dimension)), 
				new int[] {Types.INTEGER});
		
		List<PartitionKeyStatistics> results = j.query(factory.newPreparedStatementCreator(parameters), new PartitionKeyStatisticsRowMapper(dimension));
		
		return results;
	}

	public void update(PartitionKeyStatistics stats) {
		Object[] parameters = new Object[] {
				stats.getChildRecordCount(), 
				new Date(System.currentTimeMillis()),
				stats.getKey()
		};
		
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(
				updateSql(IndexSchema.getPrimaryIndexTableName(stats.getPartitionDimension())), 
				new int[] {Types.INTEGER, Types.DATE, stats.getPartitionDimension().getColumnType()});
		j.update(factory.newPreparedStatementCreator(parameters));
	}
	
	private String updateSql(String tableName) {
		StringBuilder sql = new StringBuilder("update ");
		sql.append(tableName);
		sql.append(" set secondary_index_count = ?, last_updated = ? where id = ?");
		return sql.toString();
	}
	
	private String selectSql(String tableName) {
		StringBuilder sql = new StringBuilder("select * from ");
		sql.append(tableName);
		sql.append(" where id = ? ");
		return sql.toString();
	}
	
	private String selectByNodeSql(String tableName) {
		StringBuilder sql = new StringBuilder("select * from ");
		sql.append(tableName);
		sql.append(" where node = ? ");
		return sql.toString();
	}

	public void decrementChildRecordCount(PartitionDimension dimension, Object primaryIndexKey, int increment) {
		PartitionKeyStatistics stats = findByPrimaryPartitionKey(dimension, primaryIndexKey);
		stats.setChildRecordCount( stats.getChildRecordCount() - increment);
		update(stats);
	}

	public void incrementChildRecordCount(PartitionDimension dimension, Object primaryIndexKey, int increment) {
		PartitionKeyStatistics stats = findByPrimaryPartitionKey(dimension, primaryIndexKey);
		stats.setChildRecordCount( stats.getChildRecordCount() + increment);
		update(stats);
	}
	
	protected class PartitionKeyStatisticsRowMapper implements RowMapper {
		private PartitionDimension dimension;
		public PartitionKeyStatisticsRowMapper(PartitionDimension dimension) {
			this.dimension = dimension;
		}
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			PartitionKeyStatisticsBean stats = new PartitionKeyStatisticsBean(dimension, rs.getObject("id"), rs.getDate("last_updated"));
			stats.setChildRecordCount(rs.getInt("secondary_index_count"));
			return stats;
		}
	}
}
