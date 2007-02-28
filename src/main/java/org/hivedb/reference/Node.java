///**
// * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
// * data storage systems.
// * 
// * @author Kevin Kelm (kkelm@fortress-consulting.com)
// */
//package org.hivedb.reference;
//
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.Vector;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.hivedb.HiveException;
//
//
//public class Node {
//
//	public static final String GLOBAL_NODE_NAME = "global";
//	
//	/**
//	 * INTERNAL USE ONLY -- create constructor
//	 * 
//	 * @param hive
//	 */
//	public Node( Hive hive ) {
//		this.hive = hive;		
//	} // create constructor
//
//	/**
//	 * INTERNAL USE ONLY -- database load constructor
//	 * 
//	 * @param hive
//	 * @param rset
//	 * @throws SQLException
//	 */
//	protected Node( Hive hive, java.sql.ResultSet rset ) throws SQLException {
//		this.hive = hive;
//		setId( rset.getInt( 1 ) );
//		try {
//			setName( rset.getString( 2 ));
//		} catch( Exception e ) { // unlikely to get here
//		} // try-catch
//		try {
//			setURI( rset.getString( 3 ) );
//		} catch( HiveException e ) {
//			throw new SQLException("Bad host for node found '" + rset.getString(2) + "'" );
//		} // try-catch
//		setReadOnly( rset.getBoolean( 4 ) );
//	} // load constructor 
//
//	/**
//	 * Returns the node's serial ID.
//	 * 
//	 * @return
//	 */
//	public int getId() {
//		return id;
//	}
//
//	/**
//	 * Set the node's ID.
//	 * 
//	 * @param id
//	 */
//	protected void setId(int id) {
//		this.id = id;
//	}
//	
//
//	/**
//	 * Returns the logical name of this node.
//	 * 
//	 * @return
//	 */
//	public String getName() {
//		return name;
//	}
//
//	/**
//	 * Sets the name of this node.  This isn't an operation client code is ever
//	 * likely to call.
//	 * 
//	 * @param name
//	 * @throws HiveException
//	 */
//	public void setName(String name) throws HiveException {
//		Node oldOne = hive.getNode( name );
//		if( oldOne != null && oldOne != this ) { // name already taken
//			throw new HiveException( "Node cannot be renamed to '" + name + "', name already in use." );
//		} // if
//		if( hive.getName().equals( name ) ) {
//			throw new HiveException( "Node name and Hive name cannot be the same.");
//		}
//		validateName( name );
//		this.name = name;
//	}
//
//	/**
//	 * Returns the hostname of this node.
//	 * 
//	 * @return
//	 */
//	public String getURI() {
//		return uri;
//	}
//
//	/**
//	 * Sets the hostname of this node.  Try to use IP addresses to avoid
//	 * DNS issues.
//	 * 
//	 * @param host
//	 */
//	public void setURI(String uri) throws HiveException {
//		this.uri = uri;
//	}
//
//	/**
//	 * Returns whether or not this node is currently in read-only mode.
//	 * 
//	 * @return
//	 */
//	public boolean isReadOnly() {
//		if( hive.isReadOnly() ) {
//			return true;
//		} // if
//		return readOnly;
//	}
//
//	/**
//	 * Sets whether this node is in read only mode (maintenance, etc).
//	 * 
//	 * @param readOnly
//	 */
//	public void setReadOnly(boolean readOnly) {
//		this.readOnly = readOnly;
//	}
//
//	/**
//	 * Throws an exception if the given name is not a valid name for a node.
//	 * 
//	 * @param name
//	 * @throws HiveException
//	 */
//	public static void validateName( String name ) throws HiveException {
//		Pattern p = Pattern.compile( "^\\w+$" );
//		Matcher m = p.matcher( name );
//		if( !m.matches() ) {
//			throw new HiveException( "Invalid node name '" + name + "'" );
//		} // if
//	} // validateName
//
//	/**
//	 * WARNING: INTERNAL USE ONLY.  CALL Hive.addNode() INSTEAD.
//	 * @throws SQLException
//	 */
//	public void insert() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		if( hive.getNode( getName() ) != null ) {
//			throw new SQLException( "Node '" + getName() + "' already exists." );
//		} // if
//		String sql = "INSERT INTO node_metadata( name, uri, read_only ) VALUES ( ?, ?, ? )";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql, java.sql.Statement.RETURN_GENERATED_KEYS );
//		pstmt.setString( 1, getName() );
//		pstmt.setString( 2, getURI() );
//		pstmt.setBoolean( 3, isReadOnly() );
//		pstmt.execute();
//		java.sql.ResultSet key = pstmt.getGeneratedKeys();
//		if( key.next() ) {
//			setId( key.getInt( 1 ) );
//		} // if
//		pstmt.close();
//	} // insert
//
//	/**
//	 * Updates this Node instance in the database and also increments the Hive's revision
//	 * number in the DB so that other application nodes' hive heartbeat can pick up the
//	 * changes.
//	 * 
//	 * @throws SQLException
//	 */
//	public void update() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "UPDATE node_metadata SET name = ?, uri = ?, read_only = ? WHERE id = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, getName() );
//		pstmt.setString( 2, getURI() );
//		pstmt.setBoolean( 3, isReadOnly() );
//		pstmt.setInt( 4, getId() );
//		pstmt.executeUpdate();
//		pstmt.close();
//		hive.update();
//	}
//
//	/**
//	 * Deletes a node from the database.  Will not delete a node that is in use by one or
//	 * more Resources in the hive.  
//	 * 
//	 * WARNING: DO NOT CALL THIS METHOD DIRECTLY-- USE Hive.removeNode().
//	 * 
//	 * @throws SQLException
//	 * @throws HiveException
//	 */
//	public void delete() throws SQLException, HiveException {
//		if( getName().equals (GLOBAL_NODE_NAME ) ) {
//			throw new HiveException( "Cannot delete " + GLOBAL_NODE_NAME + " node." );
//		} // if
//		
//		Vector<Resource> users = hive.getResourcesUsingNode( getName() );
//		if( users != null && users.size() > 0 ) {
//			throw new HiveException( "Cannot delete node '" + getName() + "', in use by " + users.size() + " resources." );
//		} // if
//		Vector<PartitionIndex> indexes = hive.getPartitionIndexesUsingNode( getName() );
//		if( indexes != null && indexes.size() > 0 ) {
//			throw new HiveException( "Cannot delete node '" + getName() + "', in use by " + indexes.size() + " partition indexes." );
//		} // if
//
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "DELETE FROM node_metadata WHERE name = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, getName() );
//		pstmt.execute();
//	} // delete
//	
//	/**
//	 * Loads all the nodes associated with the given hive.
//	 * @param hive
//	 * @param conn
//	 * @throws SQLException
//	 */
//	public static HashMap<String,Node> loadAll( Hive hive ) throws SQLException {
//		freeAll( hive );
//		HashMap<String,Node> nodes = new HashMap<String,Node>();
//		
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "SELECT id, name, uri, read_only  FROM node_metadata";
//		java.sql.Statement stmt = conn.createStatement();
//		java.sql.ResultSet rset = stmt.executeQuery( sql );
//		while( rset.next() ) {
//			Node node = new Node( hive, rset );
//			nodes.put( node.getName(), node );
//		} // while
//		rset.close();
//		stmt.close();
//		
//		return nodes;
//	} // loadAll
//
//	/**
//	 * To be used only by JHive internals.
//	 * 
//	 * @param hive
//	 */
//	public static void freeAll( Hive hive ) {
//		hive.setNodes( new HashMap<String, Node>() );		
//	} // freeAll
//
//	protected int     id;
//	protected String  name;
//	protected String  uri;
//	protected boolean readOnly = false;
//
//	protected Hive    hive;
//} // class Node
