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
//public class Resource {
//
//	public static final int TYPE_TABLE		= 0;
//	public static final int TYPE_PARTITIONED_TABLE    = 1;
//	public static final int TYPE_MEMCACHE	     = 2;
//	public static final int TYPE_PARTITIONED_MEMCACHE = 3;
//	
//	public static final int ACCESS_READ      = 0;
//	public static final int ACCESS_WRITE     = 1;
//	public static final int ACCESS_READWRITE = 2;
//
//	public static final int MAX_SHARE_LEVEL = 100;
//
//	/**
//	 * INTERNAL USE ONLY -- create constructor
//	 * 
//	 * @param hive
//	 */
//	public Resource( Hive hive ) {
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
//	protected Resource( Hive hive, java.sql.ResultSet rset ) throws SQLException {
//		this.hive = hive;
//
//		setId( rset.getInt( 1 ) );
//		try {
//			setName( rset.getString( 2 ));
//		} catch( Exception e ) { // unlikely to get here
//		} // try-catch
//		try {
//			setType( rset.getString( 3 ) );
//		} catch( Exception e ) { // unlikely to get here
//		} // try-catch
//		setNode( rset.getString( 4 ) );
//		try {
//			setAccess( rset.getString( 5 ) );
//		} catch( Exception e ) { // unlikely to get here
//		} // try-catch
//		setReadShareLevel( rset.getInt( 6 ) );
//		setWriteShareLevel( rset.getInt( 7 ) );
//	} // load constructor 
//	
//	/**
//	 * returns the ID of the resource.
//	 * @return
//	 */
//	public int getId() {
//		return id;
//	}
//
//	/**
//	 * Sets the ID of the resource
//	 * @param id
//	 */
//	protected void setId(int id) {
//		this.id = id;
//	}
//
//	/**
//	 * Returns the name of this resource.
//	 * 
//	 * @return
//	 */
//	public String getName() {
//		return name;
//	}
//
//	/**
//	 * Sets the name of this resource.  This isn't an operation client
//	 * code is ever likely to call.
//	 * 
//	 * @param name
//	 * @throws HiveException
//	 */
//	public void setName(String name) throws HiveException {
//// TO DO: WHAT DOES IT MEAN TO WIND UP WITH A NEW NAME?
//		validateName( name );
//		this.name = name;
//	}
//
//	/**
//	 * Returns the type of this resource using the TYPE_*
//	 * defines above.
//	 * 
//	 * @return
//	 */
//	public int getType() {
//		return type;
//	}
//
//	/** Returns the type of this resource as a human-
//	 * readable string.
//	 */
//	public String getTypeString() {
//		return typeStrings.elementAt( getType() );
//	}
//	
//	/** Returns the type of this resource as a human-
//	 * readable string.
//	 */
//	public static String getTypeString( int type ) {
//		return typeStrings.elementAt( type );
//	}
//	
//	/* Sets the type of this resource using the TYPE_*
//	 * defines above.
//	 */
//	public void setType(int type) {
//		this.type = type;
//	}
//
//	/**
//	 * Sets the type of this resource, case-insensitive.
//	 * 
//	 * @param type
//	 * @throws HiveException
//	 */
//	public void setType( String type ) throws HiveException {
//		int index = typeStrings.indexOf( type.toUpperCase() );
//		if( index < 0 ) {
//			throw new HiveException( "Invalid type '" + type + "'" );
//		} // if
//		this.type = index;
//	}
//
//	/**
//	 * Returns the name of the node at which this
//	 * resource is located.
//	 * 
//	 * @return
//	 */
//	public String getNode() {
//		return node;
//	}
//
//	/* Sets the node name at which this resource is
//	 * located.
//	 */
//	public void setNode(String node) {
//		this.node = node;
//	}
//
//	/* Returns the access of this resource as one of the
//	 * ACCESS_* defines above.
//	 */
//	public int getAccess() {
//		return access;
//	}
//
//	/**
//	 * Returns the access of this resource as a string.
//	 * 
//	 * @return
//	 */
//	public String getAccessString() {
//		return accessStrings.elementAt( getAccess() );
//	}
//	
//	/**
//	 * static version for non-object-related access to access strings...
//	 * 
//	 */
//	public static String getAccessString( int access ) {
//		return accessStrings.elementAt( access );
//	} // getAccessString
//
//	
//	/**
//	 * Sets the read/write/readwrite access of this resource
//	 * using the ACCESS_* defines above.
//	 * 
//	 * @param access
//	 */
//	public void setAccess(int access) {
//		this.access = access;
//	}
//
//	/**
//	 * Sets the read/write/readwrite access of this resource,
//	 * case-insensitive.
//	 * 
//	 * @param access
//	 * @throws HiveException
//	 */
//	public void setAccess( String access ) throws HiveException {
//		int index = accessStrings.indexOf( access.toUpperCase() );
//		if( index < 0 ) {
//			throw new HiveException( "Invalid access '" + type + "'" );
//		} // if
//		this.access = index;
//	}
//
//	/**
//	 * Returns the current read share level of this resource.
//	 * @return
//	 */
//	public int getReadShareLevel() {
//		return readShareLevel;
//	}
//
//	/**
//	 * Sets the read share level of this Resource.  This sets
//	 * the hive up to select which partition to insert a
//	 * new entity into.  All resources must have at least
//	 * one partition with a non-zero share level.
//	 * 
//	 * @param shareLevel
//	 */
//	public void setReadShareLevel(int shareLevel) {
//		if( shareLevel > MAX_SHARE_LEVEL ) {
//			shareLevel = MAX_SHARE_LEVEL;
//		} else if( shareLevel < 0 ) {
//			shareLevel = 0;
//		} // if-else
//
//		this.readShareLevel = shareLevel;
//		if( hive.getResources() != null ) {
//			Vector<Resource> list = hive.getResources().get( getName() );
//			if( list != null ) {
//				buildReadShareMap( getName(), list );
//			} // if
//		} // if
//	} // setReadShareLevel
//
//	/**
//	 * Returns the current write share level of this resource.
//	 * @return
//	 */
//	public int getWriteShareLevel() {
//		return writeShareLevel;
//	}
//
//	/**
//	 * Sets the write share level of this Resource.  This sets
//	 * the hive up to select which partition to insert a
//	 * new entity into.  All resources must have at least
//	 * one partition with a non-zero share level.
//	 * 
//	 * @param shareLevel
//	 */
//	public void setWriteShareLevel(int shareLevel) {
//		if( shareLevel > MAX_SHARE_LEVEL ) {
//			shareLevel = MAX_SHARE_LEVEL;
//		} else if( shareLevel < 0 ) {
//			shareLevel = 0;
//		} // if-else
//
//		this.writeShareLevel = shareLevel;
//		if( hive.getResources() != null ) {
//			Vector<Resource> list = hive.getResources().get( getName() );
//			if( list != null ) {
//				buildWriteShareMap( getName(), list );
//			} // if
//		} // if
//	} // setWriteShareLevel
//
//	/**
//	 * Throws an exception if the given name is not a valid Resource name.
//	 * 
//	 * @param name
//	 * @throws HiveException
//	 */
//	public static void validateName( String name ) throws HiveException {
//		Pattern p = Pattern.compile( "^\\w+$" );
//		Matcher m = p.matcher( name );
//		if( !m.matches() ) {
//			throw new HiveException( "Invalid resource name '" + name + "'" );
//		} // if
//	} // validateName
//	
//	/**
//	 * Used to update the representation of a Resource in the database.  Automatically
//	 * updates the Hive both in-memory and in the DB to reflect changes to the metadata
//	 * for the various application servers' metadata heartbeats to detect.
//	 * 
//	 * @throws SQLException
//	 */
//	public void update() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "UPDATE resource_metadata SET name = ?, type = ?, node = ?, " +
//				"access = ?, read_share_level = ?, write_share_level = ? WHERE id = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, getName() );
//		pstmt.setString( 2, typeStrings.elementAt( getType() ) );
//		pstmt.setString( 3, getNode() );
//		pstmt.setString( 4, accessStrings.elementAt( getAccess() ) );
//		pstmt.setInt( 5, getReadShareLevel() );
//		pstmt.setInt( 6, getWriteShareLevel() );
//		pstmt.setInt( 7, getId() );
//		pstmt.executeUpdate();
//		pstmt.close();
//		
//		HashMap<String, String[]> readShareMap = hive.getReadShareMap();
//		readShareMap.put( name, buildReadShareMap( name, hive.getResource( getName() ) ) );
//		HashMap<String, String[]> writeShareMap = hive.getWriteShareMap();
//		writeShareMap.put( name, buildWriteShareMap( name, hive.getResource( getName() ) ) );
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
//		String sql = "DELETE FROM resource_metadata WHERE id = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setInt( 1, getId() );
//		pstmt.execute();
//		
//		pstmt.close();
//		
//		hive.updateShareMap();
//	} // delete
//
//	/**
//	 * Inserts a resource into the hive database.
//	 * 
//	 * WARNING: DO NOT CALL THIS METHOD DIRECTLY -- USE Hive.addResource().
//	 * 
//	 * @throws SQLException
//	 */
//	public void insert() throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		String sql = "INSERT INTO resource_metadata( name, type, node, access, read_share_level, write_share_level ) "
//				+ "VALUES ( ?, ?, ?, ?, ?, ? )";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql, java.sql.Statement.RETURN_GENERATED_KEYS );
//		pstmt.setString( 1, getName() );
//		pstmt.setString( 2, getTypeString() );
//		pstmt.setString( 3, getNode() );
//		pstmt.setString( 4, getAccessString() );
//		pstmt.setInt( 5, getReadShareLevel() );
//		pstmt.setInt( 6, getWriteShareLevel() );
//		pstmt.execute();
//		java.sql.ResultSet key = pstmt.getGeneratedKeys();
//		if( key.next() ) {
//			setId( key.getInt( 1 ) );
//		} // if
//
//		pstmt.close();
//	} // addResource
//	
//	/**
//	 * For a given hive, load all the Resource entries, returning them as a HashMap
//	 * keyed on resource name, where the value is a Vector of partitions holding some
//	 * portion of that resource.  Unpartitioned resources are represented as a single-
//	 * entry Vector.
//	 * 
//	 * @param hive
//	 * @return
//	 * @throws SQLException
//	 */
//	public static HashMap<String, Vector<Resource>> loadAll( Hive hive ) throws SQLException {
//		java.sql.Connection conn = hive.getConnection();
//		HashMap<String, Vector<Resource>> resources = new HashMap<String, Vector<Resource>>();
//		
//		String sql = "SELECT id, name, type, node, access, read_share_level, write_share_level FROM resource_metadata";
//		java.sql.Statement stmt = conn.createStatement();
//		java.sql.ResultSet rset = stmt.executeQuery( sql );
//		while( rset.next() ) {
//			Resource resource = new Resource( hive, rset );
//			Vector<Resource> list = resources.get( resource.getName() );
//			if( list == null ) {
//				list = new Vector<Resource>();
//				resources.put( resource.getName(), list );
//			} // if
//			list.add( resource );
//		} // while
//		
//		stmt.close();
//
//		return resources;
//	} // loadAll
//
//	public static int count( HashMap<String, Vector<Resource>> resources ) {
//		int num = 0;
//		for( Vector list : resources.values() ) {
//			num += list.size();
//		} // for
//		
//		return num;
//	} // count
//	
//	/**
//	 * For all resources, build a HashMap of allocation maps keyed on resource
//	 * name.  This is used anywhere a new entry for a partitioned entity is to
//	 * be created: the allocation map honors the relative share levels of each
//	 * partition to randomly determine which partition to put the new entry
//	 * in.
//	 * 
//	 * @param hive
//	 * @return
//	 */
//	public static HashMap<String, String[]> buildReadShareMap( Hive hive ) {
//		HashMap<String, Vector<Resource>> resources = hive.getResources();
//		
//		HashMap<String, String[]> shareMap = new HashMap<String, String[]>();
//		for( String name : resources.keySet() ) {
//			Vector<Resource> list = resources.get( name );
//			shareMap.put( name, buildReadShareMap( name, list ) );
//		} // for
//		return shareMap;
//	} // buildReadShareMap
//
//	/**
//	 * For all resources, build a HashMap of allocation maps keyed on resource
//	 * name.  This is used anywhere a new entry for a partitioned entity is to
//	 * be created: the allocation map honors the relative share levels of each
//	 * partition to randomly determine which partition to put the new entry
//	 * in.
//	 * 
//	 * @param hive
//	 * @return
//	 */
//	public static HashMap<String, String[]> buildWriteShareMap( Hive hive ) {
//		HashMap<String, Vector<Resource>> resources = hive.getResources();
//		
//		HashMap<String, String[]> shareMap = new HashMap<String, String[]>();
//		for( String name : resources.keySet() ) {
//			Vector<Resource> list = resources.get( name );
//			shareMap.put( name, buildReadShareMap( name, list ) );
//		} // for
//		return shareMap;
//	} // buildWriteShareMap
//
//	/**
//	 * Builds the allocation map of partition entries for a given resource based
//	 * on each partition's share level.
//	 * 
//	 * @param key
//	 * @param list
//	 * @return
//	 */
//	protected static String[] buildReadShareMap( String key, Vector<Resource> list ) {
//		int totalShare = 0;
//		for( Resource resource : list ) {
//			totalShare += resource.getReadShareLevel();
//		} // for
//		String map[] = new String[totalShare];
//		int pos = 0;
//		for( Resource resource : list ) {
//			for( int i = 0; i < resource.getReadShareLevel(); i++ ) {
//				map[pos++] = resource.getNode();
//			} // for
//		} // for
//
//		return map;
//	} // buildReadShareMap
//	
//	/**
//	 * Builds the allocation map of partition entries for a given resource based
//	 * on each partition's share level.
//	 * 
//	 * @param key
//	 * @param list
//	 * @return
//	 */
//	protected static String[] buildWriteShareMap( String key, Vector<Resource> list ) {
//		int totalShare = 0;
//		for( Resource resource : list ) {
//			totalShare += resource.getWriteShareLevel();
//		} // for
//		String map[] = new String[totalShare];
//		int pos = 0;
//		for( Resource resource : list ) {
//			for( int i = 0; i < resource.getWriteShareLevel(); i++ ) {
//				map[pos++] = resource.getNode();
//			} // for
//		} // for
//
//		return map;
//	} // buildWriteShareMap
//	
//	/**
//	 * To be used only by JHive internals.
//	 * @param hive
//	 */
//	public static void freeAll( Hive hive ) {
//		hive.setResources( new HashMap<String, Vector<Resource>>() );
//		hive.updateShareMap();
//	} // freeAll
//
//
//	protected int    id;
//	protected String name;
//	protected int    type;
//	protected String node;
//	protected int    access;
//	protected int    readShareLevel;
//	protected int    writeShareLevel;
//	protected Hive   hive;
//	
//	static Vector<String> typeStrings = new Vector<String>();
//	static {
//		typeStrings.add( "TABLE" );
//		typeStrings.add( "PARTITIONED_TABLE" );
//		typeStrings.add( "MEMCACHE" );
//		typeStrings.add( "PARTITIONED_MEMCACHE" );
//	} // static initializer
//	
//	static Vector<String> accessStrings = new Vector<String>();
//	static {
//		accessStrings.add( "READ" );
//		accessStrings.add( "WRITE" );
//		accessStrings.add( "READWRITE" );
//	} // static initializer
//} // class Resource
