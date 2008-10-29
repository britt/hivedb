/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.configuration.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.hivedb.HiveRuntimeException;
import org.hivedb.Node;
import org.hivedb.Lockable.Status;
import org.hivedb.util.Lists;
import org.hivedb.util.Strings;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class NodeDao extends JdbcDaoSupport {
	public NodeDao(DataSource ds) {
		this.setDataSource(ds);
	}

	public Node create(Node newObject) {
		KeyHolder generatedKey = new GeneratedKeyHolder();
		JdbcTemplate j = getJdbcTemplate();
		
		PreparedStatementCreatorFactory creatorFactory = 
			new PreparedStatementCreatorFactory(insertSql(),getTypes());
		creatorFactory.setReturnGeneratedKeys(true);
		
		int rows = j.update(creatorFactory.newPreparedStatementCreator(getParameters(newObject)), generatedKey);
		if (rows != 1)
			throw new HiveRuntimeException("Unable to create Resource: "
					+ getParameters(newObject));
		if (generatedKey.getKeyList().size() == 0)
			throw new HiveRuntimeException("Unable to retrieve generated primary key");
		
		newObject.updateId(generatedKey.getKey().intValue());
		
		return newObject;	
	}

	public List<Node> loadAll() {
		JdbcTemplate t = getJdbcTemplate();
		ArrayList<Node> results = new ArrayList<Node>();
		for (Object result : t.query("SELECT * FROM node_metadata",
				new NodeRowMapper())) {
			results.add((Node)result);
		}
		return results;
	}

	public Node findById(int id) {
		JdbcTemplate t = getJdbcTemplate();
		return (Node) t.queryForObject("SELECT * FROM node_metadata WHERE id = ?", new Object[] { id }, new NodeRowMapper());
	}
	
	public Node update(Node node) {
		JdbcTemplate j = getJdbcTemplate();
		
		Object[] params = new Object[getFields().length+1];
		int[] types = new int[params.length];
		
		Lists.copyInto(getParameters(node), params);
		Lists.copyInto(getTypes(), types);
		params[params.length-1] = node.getId();
		types[types.length-1] = Types.INTEGER;
		
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(updateSql(),types);
		
		int rows = j.update(creatorFactory.newPreparedStatementCreator(params));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to update node with id: " + node.getId());
		
		return node;
	}
	
	public Node delete(Node node) {
		Object[] parameters = new Object[] { node.getId()};
		
		JdbcTemplate j = getJdbcTemplate();
		PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
				"DELETE from node_metadata where id=?",
				new int[] { Types.INTEGER });
		int rows = j.update(creatorFactory
				.newPreparedStatementCreator(parameters));
		if (rows != 1)
			throw new HiveRuntimeException("Unable to delete node for id: " + node.getId());
		return node;
	}

	protected class NodeRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			Node node = new Node(
							rs.getInt("id"), 
							rs.getString("name"), 
							rs.getString("database_name"), 
							rs.getString("host"), 
							rs.getString("dialect") == null ? HiveDbDialect.MySql : DialectTools.stringToDialect(rs.getString("dialect"))
			);
			node.setStatus(Status.getByValue(rs.getInt("status")));		
			node.setUsername(rs.getString("username"));
			node.setPassword(rs.getString("password"));
			node.setPort(rs.getInt("port"));
			node.setCapacity(rs.getInt("capacity"));
			node.setOptions(rs.getString("options"));
			return node;
		}
	}
	
	private String insertSql(){
		String[] questionMarks = new String[getFields().length];
		for(int i=0; i< questionMarks.length; i++)
			questionMarks[i] = "?";
		return String.format("INSERT INTO node_metadata (%s) VALUES (%s)", Strings.join(",", getFields()), Strings.join(",", questionMarks));
	}
	
	private String updateSql() {
		String[] fields = Transform.map(new Unary<String, String>(){
			public String f(String item) {
				return String.format("%s=?", item);
			}}, Lists.newList(getFields())).toArray(new String[]{});
		return String.format("UPDATE node_metadata set %s where id=?", Strings.join(",", fields));
	}
	
	//This is vulgar but maps don't have dependable ordering.
	//values() comes out in a different order than keySet()
	private String[] getFields() {
		return new String[] {
				"name",
				"database_name",
				"host",
				"dialect",
				"status",
				"username",
				"password",
				"port",
				"capacity",
				"options",
		};
	}

	private int[] getTypes() {
		return new int[] {
				Types.VARCHAR,
				Types.VARCHAR,
				Types.VARCHAR,
				Types.VARCHAR,
				Types.INTEGER,
				Types.VARCHAR,
				Types.VARCHAR,
				Types.INTEGER,
				Types.INTEGER,
				Types.VARCHAR,
		};
	}
	
	private Object[] getParameters(Node newObject) {
		return new Object[] { 
				newObject.getName(), 
				newObject.getDatabaseName(),
				newObject.getHost(),
				DialectTools.dialectToString(newObject.getDialect()),
				newObject.getStatus().getValue(),
				newObject.getUsername(),
				newObject.getPassword(),
				newObject.getPort(),
				newObject.getCapacity(),
				newObject.getOptions(),
				};
	}
}
