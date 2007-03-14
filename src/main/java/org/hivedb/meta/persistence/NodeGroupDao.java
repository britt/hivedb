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

import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class NodeGroupDao extends JdbcDaoSupport implements
		DataAccessObject<NodeGroup, Integer> {
	private DataSource ds;

	public NodeGroupDao(DataSource ds) {
		this.ds = ds;
		this.setDataSource(ds);
	}

	public Integer create(NodeGroup newObject) throws SQLException {
		Object[] parameters = new Object[] {  };
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO node_group_metadata VALUES (default)");
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new SQLException("Unable to create Node Group: " + parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new SQLException("Unable to retrieve generated primary key");
		newObject.updateId(generatedKey.getKey().intValue());
		
		// dependents
		for (Node n : newObject.getNodes())
			new NodeDao(ds).create(n);
		
		return new Integer(newObject.getId());
	}

	public List<NodeGroup> loadAll() throws SQLException {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<NodeGroup> results = new ArrayList<NodeGroup>();
		for (Object result : t.query("SELECT * FROM node_group_metadata",
				new NodeGroupRowMapper())) {
			results.add((NodeGroup) result);
		}
		return results;
	}

	protected class NodeGroupRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			final int id = rs.getInt("id");
			List<Node> nodes = new NodeDao(ds).findByGroup(id);
			NodeGroup group = new NodeGroup(id, nodes);
			return group;
		}
	}

	public void update(NodeGroup object) throws SQLException {
		// nothing to do
	}
	
	public void delete(NodeGroup nodeGroup) throws SQLException {
		// dependencies
		for (Node node : nodeGroup.getNodes())
			new NodeDao(ds).delete(node);
		
		Object[] parameters;
		parameters = new Object[] { nodeGroup.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE from node_group_metadata where id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new SQLException("Unable to delete node group for id: " + nodeGroup.getId());
	}

	public NodeGroup get(int id) {
		JdbcTemplate t = getJdbcTemplate();
		return (NodeGroup) t.queryForObject("SELECT * FROM node_group_metadata WHERE id = ?", new Object[] {Integer.valueOf(id)}, new NodeGroupRowMapper());
	}
}
