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
import org.hivedb.meta.Access;
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
				newObject.getUri(), 
				newObject.getAccess().getAccessType().toString(),
				newObject.getAccess().getReadShareLevel(),
				newObject.getAccess().getWriteShareLevel(),
				newObject.isReadOnly()
				};
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"INSERT INTO node_metadata (node_group_id,uri,access,read_share_level,write_share_level,read_only) VALUES (?,?,?,?,?,?)",
				new int[] {Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.INTEGER});
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
		for (Object result : t.query("SELECT * FROM NODE_METADATA",
				new NodeRowMapper())) {
			results.add((Node) result);
		}
		return results;
	}

	public List<Node> findByGroup(int id) {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<Node> results = new ArrayList<Node>();
		for (Object result : t.query("SELECT * FROM NODE_METADATA WHERE node_group_id = ?", new Object[] { id },
				new NodeRowMapper())) {
			results.add((Node) result);
		}
		return results;
	}

	public void update(Node node) throws SQLException {
		Object[] parameters = new Object[] { 
				node.getNodeGroup().getId(),
				node.getUri(), 
				node.getAccess().getAccessType().toString(),
				node.getAccess().getReadShareLevel(),
				node.getAccess().getWriteShareLevel(),
				node.isReadOnly(),
				node.getId()
				};
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"UPDATE node_metadata set node_group_id=?,uri=?,access=?,read_share_level=?,write_share_level=?,read_only=? where id=?",
				new int[] {Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.INTEGER,Types.INTEGER});
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new SQLException("Unable to update node with id: " + node.getId());
	}

	protected class NodeRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			Access access;
			try {
				access = new Access(
						Access.parseType(rs.getString("access")),
						rs.getInt("read_share_level"),
						rs.getInt("write_share_level"));
			} catch (HiveException e) {
				throw new SQLException(e.getMessage());
			}
			return new Node(rs.getInt("id"), rs.getString("uri"), access, rs.getInt("read_only") == 0 ? false : true);
		}
	}
}
