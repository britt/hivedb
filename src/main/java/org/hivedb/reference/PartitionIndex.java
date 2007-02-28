///**
// * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
// * data storage systems.
// * 
// * @author Kevin Kelm (kkelm@fortress-consulting.com)
// */
//package org.hivedb.reference;
//
//import java.sql.*;
//import java.util.*;
//
//import org.hivedb.*;
//
//public class PartitionIndex {
//
//	public static final int LEVEL_PRIMARY   = 0;
//	public static final int LEVEL_SECONDARY = 1;
//	public static final int LEVEL_LINKED    = 2;
//	
//	public static final int DATATYPE_INT    = 0;
//	public static final int DATATYPE_LONG   = 1;
//	public static final int DATATYPE_FLOAT  = 2;
//	public static final int DATATYPE_DOUBLE = 3;
//	public static final int DATATYPE_STRING = 4;
//	public static final int DATATYPE_DATE   = 5;
//	
//	/**
//	 * INTERNAL USE ONLY -- create constructor
//	 * 
//	 * @param hive
//	 */
//	public PartitionIndex( Hive hive ) {
//		this.hive = hive;	
//	} // create constructor
//
//	/**
//	 * INTERNAL USE ONLY -- load constructor
//	 * 
//	 * @param hive
//	 * @param rset
//	 * @throws SQLException
//	 */
//	protected PartitionIndex( Hive hive, java.sql.ResultSet rset ) throws HiveException, SQLException {
//		this.hive = hive;
//
//		setId( rset.getInt( 1 ) );
//		setResource( rset.getString( 2 ) );
//		setNode( rset.getString( 3 ) );
//		setLevel( rset.getString( 4 ) );
//		setColumnName( rset.getString( 5 ) );
//		try {
//			setDataType( rset.getString( 6 ) );
//		} catch( HiveException he ) {
//			throw new SQLException( he.getMessage() );
//		} // try-catch
//	} // load constructor 
//
//	public int getId() {
//		return id;
//	}
//
//	public void setId(int id) {
//		this.id = id;
//	}
//
//	public String getResource() {
//		return resource;
//	}
//
//	public void setResource(String resource) {
//		this.resource = resource;
//	}
//
//	public String getNode() {
//		return node;
//	}
//
//	public String getTable() {
//		return node;
//	}
//
//	public void setNode(String node) {
//		this.node = node;
//	}
//
//	public void setTable(String table) {
//		this.node = table;
//	}
//
//	public int getLevel() {
//		return level;
//	}
//
//	public void setLevel(int level) {
//		this.level = level;
//	}
//
//	public void setLevel(String level) throws HiveException {
//		int index = typeStrings.indexOf( level.toUpperCase() );
//		if( index < 0 ) {
//			throw new HiveException( "Invalid level '" + level + "'" );
//		} // if
//		this.level = index;
//	}
//
//	public String getLevelString() {
//		return typeStrings.elementAt( level );
//	}
//	
//	public String getColumnName() {
//		return columnName;
//	}
//
//	public void setColumnName(String columnName) {
//		this.columnName = columnName;
//	}
//
//	public int getDataType() {
//		return dataType;
//	}
//
//	public String getDataTypeString() {
//		return dataTypeStrings.elementAt( getDataType() );
//	}
//	
//	public String getDBDataTypeString() {
//		return dbDataTypeStrings.elementAt( getDataType() );
//	}
//	
//	public void setDataType(int dataType ) {
//		this.dataType = dataType;
//	}
//	
//	public void setDataType(String dataType) throws HiveException {
//		int index = dataTypeStrings.indexOf( dataType.toUpperCase() );
//		if( index < 0 ) {
//			throw new HiveException( "Invalid data type '" + dataType + "'" );
//		} // if
//		this.dataType = index;
//	}
//
//	/**
//	 * Inserts a partition index into the hive database.
//	 * 
//	 * WARNING: DO NOT CALL THIS METHOD DIRECTLY -- USE Hive.addPartitionIndex().
//	 * 
//	 * @throws SQLException
//	 */
//	public void insert() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "INSERT INTO partition_index_metadata( resource, node, level, column_name, type ) "
//				+ "VALUES ( ?, ?, ?, ?, ? )";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql, java.sql.Statement.RETURN_GENERATED_KEYS );
//		pstmt.setString( 1, getResource() );
//		pstmt.setString( 2, getNode() );
//		pstmt.setString( 3, getLevelString() );
//		pstmt.setString( 4, getColumnName() );
//		pstmt.setString( 5, getDataTypeString() );
//		pstmt.execute();
//		java.sql.ResultSet key = pstmt.getGeneratedKeys();
//		if( key.next() ) {
//			setId( key.getInt( 1 ) );
//		} // if
//		pstmt.close();
//	} // addResource
//
//	/**
//	 * Used to update the representation of a PartitionIndex in the database.  Automatically
//	 * updates the Hive both in-memory and in the DB to reflect changes to the metadata
//	 * for the various application server's metadata heartbeats to detect.
//	 * 
//	 * @throws SQLException
//	 */
//	public void update() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "UPDATE partition_index_metadata SET resource = ?, node = ?, level = ?, " +
//				"column_name = ?, type = ? WHERE id = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, getResource() );
//		pstmt.setString( 2, getNode() );
//		pstmt.setString( 3, getLevelString() );
//		pstmt.setString( 4, getColumnName() );
//		pstmt.setString( 5, getDataTypeString() );
//		pstmt.setInt( 6, getId() );
//		pstmt.executeUpdate();
//		pstmt.close();
//		
//		hive.update();
//	} // update
//
//	/**
//	 * deletes a resource from the hive database.
//	 * 
//	 * WARNING: DO NOT CALL THIS METHOD DIRECTLY -- USE Hive.removeResource().
//	 * 
//	 * @throws SQLException
//	 */
//	public void delete() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "DELETE FROM partition_index_metadata WHERE id = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setInt( 1, getId() );
//		pstmt.execute();
//		
//		pstmt.close();
//
//	} // delete
//
//	public static String makeKey( String index, String columnName ) {
//		return index + ":" + columnName;
//	} // makeKey
//	
//	/**
//	 * For a given hive, load all the PartitionIndexes, returning them as a HashMap
//	 * keyed on resource name + ":" + column name, where the value is a PartitionIndex
//	 * object. 
//	 * 
//	 * @param hive
//	 * @return
//	 * @throws SQLException
//	 */
//	public static HashMap<String, PartitionIndex> loadAll( Hive hive ) throws HiveException, SQLException {
//		freeAll( hive );
//		java.sql.Connection conn = hive.getConnection();
//		HashMap<String, PartitionIndex> indexes = new HashMap<String, PartitionIndex>();
//		
//		String sql = "SELECT id, resource, node, level, column_name, type FROM partition_index_metadata";
//		java.sql.Statement stmt = conn.createStatement();
//		java.sql.ResultSet rset = stmt.executeQuery( sql );
//		while( rset.next() ) {
//			PartitionIndex index = new PartitionIndex( hive, rset );
//			indexes.put( makeKey( index.getResource(), index.getColumnName() ), index );
//		} // while
//		stmt.close();
//		
//		return indexes;
//	} // loadAll
//	
//	public static HashMap<String,PartitionIndex> getPrimaryIndexes( Hive hive ) {
//		HashMap<String, PartitionIndex> indexes = hive.getPartitionIndexes();
//		HashMap<String, PartitionIndex> primaries = new HashMap<String, PartitionIndex>();
//		for( PartitionIndex pi : indexes.values() ) {
//			if( pi.getLevel() == LEVEL_PRIMARY ) {
//				primaries.put( pi.getResource(), pi );
//			} // if
//		} // for
//		return primaries;
//	} // getPrimaryIndexes
//	
//	/**
//	 * For use by Hive only-- returns information about the node a partitioned object
//	 * keyed on 'value' lives on.  Returns node name and readOnly status.
//	 * @param value
//	 * @return
//	 */
//	public PartitionedObjectStatus getNodeNameForPrimaryID( Object value ) throws SQLException, HiveException {
//		java.sql.Connection conn = null;
//		java.sql.PreparedStatement stmt = null;
//		try {
//			conn = hive.getConnectionForNode( getNode(), Resource.ACCESS_READ );
//			String sql = "SELECT node, read_only FROM " + makePartitionIndexName() + " WHERE id = ?";
//			stmt = conn.prepareStatement( sql );
//			setValue( stmt, 1, getDataType(), value );
//			java.sql.ResultSet rset = stmt.executeQuery();
//			if( !rset.next() ) {
//				throw new HiveException( "Object not found" );
//			} // if
//			String nodeName = rset.getString( 1 );
//			boolean readOnly = rset.getBoolean( 2 );
//			return new PartitionedObjectStatus( nodeName, readOnly, "" + value, true );
//		} finally {
//			if( stmt != null ) {
//				stmt.close();
//			} // if
//			if( conn != null ) {
//				conn.close();
//			} // if
//		} // try-catch-finally
//	} // getNodeNameForID
//
//	/**
//	 * For use by Hive only-- Look up the value in a JOINed query since both
//	 * indexes are on the same node.  Returns nodename and readOnly status.
//	 * @param primary
//	 * @param secondary
//	 * @return
//	 */
//	public static PartitionedObjectStatus getJoinedNodeName( Hive hive, PartitionIndex primary, PartitionIndex secondary, Object value ) throws HiveException, SQLException {
//		java.sql.Connection conn = null;
//		java.sql.PreparedStatement stmt = null;
//		try {
//			conn = hive.getConnectionForNode( primary.getNode(), Resource.ACCESS_READ );
//			String sql = "SELECT pr.node, pr.read_only " +
//					"FROM " + primary.makePartitionIndexName() + " pr, " +
//					secondary.makePartitionIndexName() + " se " +
//					"WHERE se.id = ? AND se.pkey = pr.id";
//			stmt = conn.prepareStatement( sql );
//			setValue( stmt, 1, secondary.getDataType(), value );
//			java.sql.ResultSet rset = stmt.executeQuery();
//			if( !rset.next() ) {
//				throw new HiveException( "Object not found" );
//			} // if
//			String nodeName = rset.getString( 1 );
//			boolean readOnly = rset.getBoolean( 2 );
//			return new PartitionedObjectStatus( nodeName, readOnly, "" + value, true );
//		} finally {
//			if( stmt != null ) {
//				stmt.close();
//			} // if
//			if( conn != null ) {
//				conn.close();
//			} // if
//		} // try-catch-finally
//	} // getJoinedNodeName
//	
//	/**
//	 * For use by Hive only-- Looks up the primary key given the secondary
//	 * key value.  Used when primary and secondary indexes are not on the same node.
//	 * @param value
//	 * @return
//	 */
//	public Object getPartitionRedirect( Object value ) throws SQLException, HiveException {
//		java.sql.Connection conn = null;
//		java.sql.PreparedStatement stmt = null;
//		try {
//			conn = hive.getConnectionForNode( getNode(), Resource.ACCESS_READ );
//			String sql = "SELECT pkey FROM " + makePartitionIndexName() + " WHERE id = ?";
//			stmt = conn.prepareStatement( sql );
//			setValue( stmt, 1, getDataType(), value );
//			java.sql.ResultSet rset = stmt.executeQuery();
//			if( !rset.next() ) {
//				throw new HiveException( "Object not found" );
//			} // if
//			switch( getDataType() ) {
//			case DATATYPE_INT:
//				return new Integer( rset.getInt( 1 ) );
//			case DATATYPE_LONG:
//				return new Long( rset.getLong( 1 ) );
//			case DATATYPE_FLOAT:
//				return new Float( rset.getFloat( 1 ) );
//			case DATATYPE_DOUBLE:
//				return new Double( rset.getDouble( 1 ) );
//			case DATATYPE_STRING:
//				return rset.getString( 1 );
//			case DATATYPE_DATE:
//				Calendar cal = Calendar.getInstance();
//				cal.setTime( rset.getDate( 1 ) );
//				return cal;
//			default:
//				throw new HiveException( "INVALID DATA TYPE IN PARTITION INDEX" );
//			} // switch
//		} finally {
//			if( stmt != null ) {
//				stmt.close();
//			} // if
//			if( conn != null ) {
//				conn.close();
//			} // if
//		} // try-catch-finally
//	} // getPartitionRedirect
//	
//	/**
//	 * Insets a new primary partitionindex entry for the given table/column.
//	 * @param nodeName
//	 * @param value
//	 * @throws HiveException
//	 * @throws HiveReadOnlyException
//	 * @throws SQLException
//	 */
//	public void insertPrimaryKey( String nodeName, Object value ) throws HiveException, HiveReadOnlyException, SQLException {
//		if( getLevel() != LEVEL_PRIMARY ) {
//			throw new HiveException( "'" + getColumnName() + "' is not a primary key in insertPrimaryKey()." );
//		} // if
//		java.sql.Connection conn = hive.getDBConnection( getNode(), Resource.ACCESS_WRITE );
//		String sql = "INSERT INTO " + makePartitionIndexName() + " ( id, node, read_only ) VALUES ( ?, ?, 0 )";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		setValue( pstmt, 1, getDataType(), value );
//		pstmt.setString( 2, nodeName );
//		pstmt.execute();
//		pstmt.close();
//		conn.close();
//	} // insertPrimaryKey
//	
//	public void insertSecondaryKey( Object primaryKeyValue, Object myValue ) throws HiveException, HiveReadOnlyException, SQLException {
//		if( getLevel() == LEVEL_PRIMARY ) {
//			throw new HiveException( "'" + getColumnName() + "' cannot be a primary key in insertSecondaryKey()." );
//		} // if
//		PartitionIndex primary = hive.getPrimaryPartitionIndex( getResource() );
//		java.sql.Connection conn = hive.getDBConnection( getNode(), Resource.ACCESS_WRITE );
//		String sql = "INSERT INTO " + makePartitionIndexName() + " ( id, pkey ) VALUES ( ?, ? )";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		setValue( pstmt, 1, getDataType(),  myValue );
//		setValue( pstmt, 2, primary.getDataType(), primaryKeyValue );
//		pstmt.execute();
//		pstmt.close();
//		conn.close();
//	} // insertSecondaryKey
//
//	public void updateSecondaryKey( Object primaryKeyValue, Object myValue ) throws HiveException, HiveReadOnlyException, SQLException {
//		if( getLevel() == LEVEL_PRIMARY ) {
//			throw new HiveException( "'" + getColumnName() + "' cannot be a primary key in insertSecondaryKey()." );
//		} // if
//		PartitionIndex primary = hive.getPrimaryPartitionIndex( getResource() );
//		java.sql.Connection conn = hive.getDBConnection( getNode(), Resource.ACCESS_WRITE );
//		String sql = "UPDATE " + makePartitionIndexName() + " SET id = ? WHERE pkey = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		setValue( pstmt, 1, getDataType(), myValue );
//		setValue( pstmt, 2, primary.getDataType(), primaryKeyValue );
//		pstmt.execute();
//		pstmt.close();
//		conn.close();
//	} // updateSecondaryKey
//
//	public void removeKey( Object value ) throws HiveException, HiveReadOnlyException, SQLException {
//		java.sql.Connection conn = hive.getDBConnection( getNode(), Resource.ACCESS_WRITE );
//		String sql = "DELETE FROM " + makePartitionIndexName() + " WHERE id = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		setValue( pstmt, 1, getDataType(), value );
//		pstmt.execute();
//		pstmt.close();
//		conn.close();
//	} // removeKey
//
//	/**
//	 * Returns the name of the table for this partition index.
//	 * @return
//	 */
//	protected String makePartitionIndexName() {
//		return "hive_index_" + getResource().toLowerCase() + "_" + getColumnName().toLowerCase();
//	} // makePartitionIndexName
//	
//	/**
//	 * For use by util.Tools only-- creates a partion index on the indicated node.
//	 * 
//	 * @param hive
//	 * @param pi
//	 * @throws HiveException
//	 * @throws SQLException
//	 */
//	public static void createIndex( Hive hive, PartitionIndex pi, PartitionIndex primary ) throws HiveException, SQLException {
//		if( pi.getLevel() == LEVEL_LINKED ) { // linked indexes just rely on the primary index, so no work needs to be done here.
//			return;
//		} // if
//
//		String node = pi.getNode();
//		java.sql.Connection conn = hive.getConnectionForNode( node, Resource.ACCESS_WRITE );
//		String sql;
//		String name = pi.makePartitionIndexName();
//		java.sql.PreparedStatement stmt = null;
//		String keyType = pi.getDBDataTypeString(); 
//		if( pi.getLevel() == LEVEL_PRIMARY ) { // create primary index
//			sql = "CREATE TABLE " + name + " ( " + 
//						"id " + keyType + " not null, " +
//						"node varchar(64) not null, " +
//						"read_only boolean default 0," +
//						"PRIMARY KEY (id) " +
//					" )";
//			stmt = conn.prepareStatement( sql );
//			stmt.execute();		
//			stmt.close();
//		} else if( pi.getLevel() == LEVEL_SECONDARY ){  // create secondary index
//			sql = "CREATE TABLE " + name + " ( " + 
//						"id " + keyType + " not null, " +
//						"pkey " + primary.getDBDataTypeString() + " not null, " +
//						"PRIMARY KEY (id) " +
//					" )";
//			stmt = conn.prepareStatement( sql );
//			stmt.execute();		
//			stmt.close();
//		} // if-else
//		
//	} // createIndex
//
//	/**
//	 * To be used only by JHive internals.
//	 * 
//	 * @param hive
//	 */
//	public static void freeAll( Hive hive ) {
//		hive.setPartitionIndexes( new HashMap<String, PartitionIndex>() );		
//	} // freeAll
//	
//	/**
//	 * Internal use only-- sets the field in the indicated prepared statement
//	 * @param stmt
//	 * @param index
//	 * @param dataType
//	 * @param value
//	 * @throws SQLException
//	 * @throws HiveException
//	 */
//	protected static void setValue( java.sql.PreparedStatement stmt, int index, int dataType, Object value ) throws SQLException, HiveException {
//		switch( dataType ) {
//		case DATATYPE_INT:
//			stmt.setInt( index, (Integer)value );
//			break;
//		case DATATYPE_LONG:
//			stmt.setLong( index, (Long)value );
//			break;
//		case DATATYPE_FLOAT:
//			stmt.setFloat( index, (Float)value );
//			break;
//		case DATATYPE_DOUBLE:
//			stmt.setDouble( index, (Double)value );
//			break;
//		case DATATYPE_STRING:
//			stmt.setString( index, (String)value );
//			break;
//		case DATATYPE_DATE:
//			java.sql.Date date = new java.sql.Date(((Calendar)value).getTime().getTime() );
//			stmt.setDate( index, date );
//			break;
//		default:
//			throw new HiveException( "INVALID DATA TYPE IN PARTITION INDEX" );
//		} // switch
//	} // setValue
//	
//
//	protected Hive    hive;
//	protected int     id;
//	protected String  resource;
//	protected String  node;
//	protected int     level;
//	protected String  columnName;
//	protected int     dataType;
//
//	static Vector<String> typeStrings = new Vector<String>();
//	static {
//		typeStrings.add( "PRIMARY" );
//		typeStrings.add( "SECONDARY" );
//		typeStrings.add( "LINKED" );
//	} // static initializer
//	
//	static Vector<String> dataTypeStrings = new Vector<String>();
//	static {
//		dataTypeStrings.add( "INT" );
//		dataTypeStrings.add( "LONG" );
//		dataTypeStrings.add( "FLOAT" );
//		dataTypeStrings.add( "DOUBLE" );
//		dataTypeStrings.add( "STRING" );
//		dataTypeStrings.add( "DATE" );
//	} // static initializer
//
//	static Vector<String> dbDataTypeStrings = new Vector<String>();
//	static {
//		dbDataTypeStrings.add( "INT" );
//		dbDataTypeStrings.add( "BIGINT" );
//		dbDataTypeStrings.add( "FLOAT" );
//		dbDataTypeStrings.add( "DOUBLE" );
//		dbDataTypeStrings.add( "VARCHAR(255)" );
//		dbDataTypeStrings.add( "DATETIME" );
//	} // static initializer
//
//
//} // class PartitionIndex
