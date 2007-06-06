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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class NodeDao extends JdbcDaoSupport implements DataAccessObject<Node,Integer> {
	public NodeDao(DataSource ds) {
		this.setDataSource(ds);
	}

	public Integer create(Node newObject) throws SQLException {
		Object[] parameters = new Object[] { 
				newObject.getNodeGroup().getId(),
				newObject.getName(), 
				newObject.getUri(), 
				newObject.isReadOnly()
				};
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO node_metadata (node_group_id,name,uri,read_only) VALUES (?,?,?,?)",
				new int[] {Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER});
		creatorFactory.setReturnGeneratedKeys(true);
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters), generatedKey);
		if (rows != 1)
			throw new SQLException("Unable to create Resource: "
					+ parameters);
		if (generatedKey.getKeyList().size() == 0)
			throw new SQLException("Unable to retrieve generated primary key");
		newObject.updateId(generatedKey.getKey().intValue());
		return new Integer(newObject.getId());	
	}

	public List<Node> loadAll() throws SQLException {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<Node> results = new ArrayList<Node>();
		for (Object result : t.query("SELECT * FROM node_metadata",
				new NodeRowMapper())) {
			results.add((Node) result);
		}
		return results;
	}

	public List<Node> findByGroup(int id) {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<Node> results = new ArrayList<Node>();
		for (Object result : t.query("SELECT * FROM node_metadata WHERE node_group_id = ?", new Object[] { id },
				new NodeRowMapper())) {
			results.add((Node) result);
		}
		return results;
	}

	public Node findById(int id) {
		JdbcTemplate t = getJdbcTemplate();
		return (Node) t.queryForObject("SELECT * FROM node_metadata WHERE id = ?", new Object[] { id }, new NodeRowMapper());
	}
	
	public void update(Node node) throws SQLException {
		Object[] parameters = new Object[] { 
				node.getNodeGroup().getId(),
				node.getName(), 
				node.getUri(), 
				node.isReadOnly(),
				node.getId()
				};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE node_metadata set node_group_id=?,name=?,uri=?,read_only=? where id=?",
				new int[] {Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.INTEGER});
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new SQLException("Unable to update node with id: " + node.getId());
	}
	
	public void delete(Node node) throws SQLException {
		Object[] parameters;
		parameters = new Object[] { node.getId()};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE from node_metadata where id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new SQLException("Unable to delete node for id: " + node.getId());
	}

	protected class NodeRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return new Node(rs.getInt("id"), rs.getString("name"), rs.getString("uri"), rs.getInt("read_only") == 0 ? false : true);
		}
	}
}
