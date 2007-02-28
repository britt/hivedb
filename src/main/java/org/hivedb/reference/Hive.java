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
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.hivedb.*;
//
//public class Hive {
//
//	public static final String VERSION = "1.0.9";
//	
//	/**
//	 * Loads the hive in the given database connection.
//	 * 
//	 * @param conn
//	 * @throws SQLException
//	 */
//	public Hive( java.sql.Connection conn ) throws SQLException, HiveException {
//		this.conn = conn;
//		reload();
//		hives.put( getName(), this );
//	} // constructor
//
//	/**
//	 * Returns the name of this hive.
//	 * 
//	 * @return
//	 */
//	public String getName() {
//		return name;
//	}
//	
//	/**
//	 * INTERNAL USE ONLY-- sets the name of this hive.
//	 * 
//	 * @param name
//	 */
//	public void setName(String name) {
//		this.name = name;
//	}
//	
//	/**
//	 * Returns whether or not  this hive is in read only mode.
//	 * @return
//	 */
//	public boolean isReadOnly() {
//		return readOnly;
//	}
//	
//	/**
//	 * Sets whether or not this hive is in read only mode.
//	 * 
//	 * @param readOnly
//	 */
//	public void setReadOnly(boolean readOnly) {
//		this.readOnly = readOnly;
//	}
//	
//	/**
//	 * Returns the revision number of this hive.
//	 * 
//	 * @return
//	 */
//	public long getRevisionNumber() {
//		return revisionNumber;
//	}
//	
//	/**
//	 * INTERNAL USE ONLY-- sets the revision number
//	 * of this hive.
//	 * @param revisionNumber
//	 */
//	protected void setRevisionNumber(long revisionNumber) {
//		this.revisionNumber = revisionNumber;
//	}
//
//	/**
//	 * Returns the hive associated with the given hive name.
//	 * 
//	 * @param name
//	 * @return
//	 */
//	public static Hive getHive( String name ) {
//		return hives.get( name );
//	}
//	
//	/**
//	 * Throws an exception if the given name is not a valid hive name.
//	 * 
//	 * @param name
//	 * @throws HiveException
//	 */
//	public static void validateName( String name ) throws HiveException {
//		Pattern p = Pattern.compile( "^\\w+$" );
//		Matcher m = p.matcher( name );
//		if( !m.matches() ) {
//			throw new HiveException( "Invalid hive name '" + name + "'" );
//		} // if
//	} // validateName
//	
//	/**
//	 * INTERNAL USE ONLY-- updates the hive metadata in the database,
//	 * bumping the revision number so that other application servers
//	 * know to reload.
//	 * 
//	 * @throws SQLException
//	 */
//	public void update() throws SQLException {
//		String sql = "UPDATE hive_metadata SET name = ?, read_only = ?, revision = ?";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, getName() );
//		pstmt.setBoolean( 2, isReadOnly() );
//		setRevisionNumber( getRevisionNumber() + 1 );
//		pstmt.setLong( 3, getRevisionNumber() );
//		pstmt.executeUpdate();
//		pstmt.close();
//	}
//	
//	/**
//	 * INTERNAL USE ONLY-- reload all hive management data from the
//	 * database.
//	 * @throws SQLException
//	 */
//	public void reload() throws SQLException, HiveException {
//		if( conn == null ) {
//			throw new HiveException( "Hive database has not been opened." );
//		} // if
//
//		java.sql.Statement stmt = conn.createStatement();
//		String sql = "SELECT name, read_only, revision FROM hive_metadata";
//		java.sql.ResultSet rset = stmt.executeQuery( sql );
//		if( !rset.next() ) {
//			throw new SQLException( "Hive metadata not found." );
//		} // if
//		
//		setName( rset.getString( 1 ) );
//		setReadOnly( rset.getBoolean( 2 ) );
//		setRevisionNumber( rset.getLong( 3 ) );
//
//		rset.close();
//		stmt.close();
//		
//		nodes = Node.loadAll( this );
//		resources = Resource.loadAll( this );
//		resourceCount = Resource.count( resources );
//		partitionIndexes = PartitionIndex.loadAll( this );
//		primaryIndexes = PartitionIndex.getPrimaryIndexes( this );
//		updateShareMap();
//	} // reload
//
//	/**
//	 * INTERNAL USE ONLY-- updates the share map to reflect
//	 * recent changes to share levels/read only statuses
//	 */
//	public void updateShareMap() {
//		readShareMap = Resource.buildReadShareMap( this );
//		writeShareMap = Resource.buildWriteShareMap( this );
//	} // updateShareMap
//	
//	/**
//	 * INTERNAL USE ONLY-- Returns the persistent database
//	 * connection used by the hive for its internal accounting.
//	 * @return
//	 */
//	public java.sql.Connection getConnection() {
//		return conn;
//	} // getConnection
//
//	/**
//	 * INTERNAL USE ONLY-- adds the given node to the hive.
//	 * @param node
//	 * @throws SQLException
//	 */
//	public void addNode( Node node ) throws SQLException {
//		node.insert();
//		nodes.put( node.getName(), node );
//		update();
//	} // addNode
//
//	/**
//	 * INTERNAL USE ONLY-- removes the given node from the hive permanently.
//	 * 
//	 * @param node
//	 * @throws SQLException
//	 * @throws HiveException
//	 */
//	public void removeNode( Node node ) throws SQLException, HiveException {
//		node.delete();
//		nodes.remove( node );
//	} // removeNode
//	
//	/**
//	 * Return the number of nodes managed by this hive.
//	 * 
//	 * @return
//	 */
//	public int getNodeCount() {
//		return nodes.size();
//	} // getCount
//
//	/**
//	 * INTERNAL USE ONLY-- add a resource to the hive.
//	 * 
//	 * @param resource
//	 * @throws SQLException
//	 */
//	public void addResource( Resource resource ) throws SQLException {
//		Vector<Resource> list = resources.get( resource.getName () );
//		if( list != null ) { // see if the declaration already exists...
//			for( Resource res : list ) {
//				if( res.getType() == resource.getType()
//							&& res.getNode().equals( resource.getNode() )
//							&& res.getAccess() == resource.getAccess() ) {
//					throw new SQLException( "Resource '" + res.getName()
//								+ "' of type '" + res.getTypeString()
//								+ "' at node '" + res.getNode()
//								+ " with access '" + res.getAccessString()
//								+ "' already exists." );
//				} // if
//			} // for
//		} // if
//
//		resource.insert();
//		if( list == null ) {
//			list = new Vector<Resource>();
//			resources.put( resource.getName(), list );
//		} // if
//		list.add( resource );
//		resourceCount++;
//		update();
//		
//		updateShareMap();
//	} // addResource
//
//	/**
//	 * INTERNAL USE ONLY-- remove a resource from the hive permanently.
//	 * 
//	 * @param resource
//	 * @throws SQLException
//	 */
//	public void removeResource( Resource resource ) throws SQLException {
//		resource.delete();
//		
//		Vector<Resource> list = resources.get( this.getName() );
//		if( list == null ) {
//			return;
//		} // if
//		list.remove( this );
//		if( list.size() == 0 ) {
//			resources.remove( list );
//		} // if
//		
//		resourceCount--;
//	} // removeNode
//	
//	/**
//	 * Given the name of a resource, returns the name of the node to store the
//	 * next new instance on, using the shareLevel-based share maps.
//	 * @param name
//	 * @return
//	 * @throws HiveException
//	 */
//	public String selectResourcePartition( String name ) throws HiveException {
//		String[] map = writeShareMap.get( name );
//		if( map == null ) {
//			throw new HiveException( "Invalid resource name '" + name + "'" );
//		} // if
//		int max = map.length;
//		if( max == 0 ) {
//			throw new HiveException( "No partitions for resource '" + name + "' have a non-zero share level." );
//		} // if
//		int pos = (int)(Math.random() * max);
//
//		return map[pos];
//	} // selectResourcePartition
//
//	/**
//	 * INTERNAL USE ONLY-- returns the list of resources residing on
//	 * the given logical node name.
//	 * 
//	 * @param name
//	 * @return
//	 */
//	public Vector<Resource> getResourcesUsingNode( String name ) {
//		Vector<Resource> out = new Vector<Resource>();
//		synchronized( resources ) {
//			for( Vector<Resource> list : resources.values() ) {
//				for( Resource res : list ) {
//					if( res.getNode().equals( name ) ) {
//						out.add( res );
//					} // if
//				} // for
//			} // for
//		} // synchronized
//		
//		return out;
//	} // getResourcesUsingNode
//
//	/**
//	 * INTERNAL USE ONLY-- returns the list of partition indexes residing on
//	 * the given logical node name.
//	 * 
//	 * @param name
//	 * @return
//	 */
//	public Vector<PartitionIndex> getPartitionIndexesUsingNode( String name ) {
//		Vector<PartitionIndex> out = new Vector<PartitionIndex>();
//		synchronized( partitionIndexes ) {
//			for( PartitionIndex pi : partitionIndexes.values() ) {
//				if( pi.getNode().equals( name ) ) {
//					out.add( pi );
//				} // if
//			} // for
//		} // synchronized
//		
//		return out;
//	} // getPartitionIndexesUsingNode
//
//	/**
//	 * INTERNAL USE ONLY-- adds the given node to the hive.
//	 * @param node
//	 * @throws SQLException
//	 */
//	public void addPartitionIndex( PartitionIndex pi ) throws SQLException, HiveException {
//		if( pi.getLevel() == PartitionIndex.LEVEL_PRIMARY ) {
//			PartitionIndex existingPrimary = primaryIndexes.get( pi.getResource() );
//			if( existingPrimary != null && existingPrimary != pi ) {
//				throw new HiveException( "A primary partition index already exists for resource '" + pi.getResource() + "'" );
//			} // if
//		} // if
//		pi.insert();
//		partitionIndexes.put( PartitionIndex.makeKey( pi.getResource(), pi.getColumnName() ), pi );
//		if( pi.getLevel() == PartitionIndex.LEVEL_PRIMARY ) {
//			primaryIndexes.put( pi.getResource(), pi );
//		} // if
//		update();
//	} // addPartitionIndex
//
//	/**
//	 * INTERNAL USE ONLY-- removes the given node from the hive permanently.
//	 * 
//	 * @param node
//	 * @throws SQLException
//	 * @throws HiveException
//	 */
//	public void removePartitionIndex( PartitionIndex pi ) throws SQLException, HiveException {
//		if( pi.getLevel() == PartitionIndex.LEVEL_PRIMARY ) { // make sure we're not deleting a primary used by secondaries
//			for( PartitionIndex pi2 : partitionIndexes.values() ) {
//				if( pi2 == pi ) {
//					continue;
//				} // if
//				if( pi2.getResource().equals( pi.getResource() ) ) {
//					throw new HiveException( "Cannot delete primary partition index for resource '" + pi.getResource()
//								+ "' until all secondary/tertiary indexes are deleted." );
//				} // if
//			} // for
//		} // if
//		pi.delete();
//		partitionIndexes.remove( pi );
//	} // removePartitionIndex
//	
//	/**
//	 * Returns the number of partition indexes in the hive.
//	 * @return
//	 */
//	public int getPartitionIndexCount() {
//		return partitionIndexes.size();
//	} // getPartitionIndexCount
//	
//	/**
//	 * INTERNAL USE ONLY-- returns the read share map for new resources in
//	 * this hive.
//	 * 
//	 * @return
//	 */
//	public HashMap<String, String[]> getReadShareMap() {
//		return readShareMap;
//	} // getReadShareMap
//
//	/**
//	 * INTERNAL USE ONLY-- returns the read share map for new resources in
//	 * this hive.
//	 * 
//	 * @return
//	 */
//	public HashMap<String, String[]> getWriteShareMap() {
//		return writeShareMap;
//	} // getWriteShareMap
//
//	/**
//	 * INTERNAL USE ONLY-- returns the resources managed by this hive.
//	 * 
//	 * @return
//	 */
//	public HashMap<String,Vector<Resource>> getResources() {
//		return resources;
//	} // getResource
//
//	/**
//	 * Returns the number of resources managed by this hive.
//	 * 
//	 * @return
//	 */
//	public int getResourceCount() {
//		return resourceCount;
//	} // getResourceCount
//
//	/**
//	 * INTERNAL USE ONLY-- returns the node list for this hive.
//	 * 
//	 * @return
//	 */
//	public HashMap<String,Node> getNodes() {
//		return nodes;
//	}
//
//	/**
//	 * INTERNAL USE ONLY-- sets the node list for this hive.
//	 * 
//	 * @param nodes
//	 */
//	public void setNodes(HashMap<String, Node> nodes) {
//		this.nodes = nodes;
//	}
//
//	/**
//	 * INTERNAL USE ONLY-- sets the resource list for this hive.
//	 * 
//	 * @param resources
//	 */
//	public void setResources(HashMap<String, Vector<Resource>> resources) {
//		this.resources = resources;
//	}
//
//	/**
//	 * Returns the Node object with the given logical name.
//	 * 
//	 * @param name
//	 * @return
//	 */
//	public Node getNode( String name ) {
//		return nodes.get( name );
//	} // getNode
//
//	/**
//	 * Returns the list of Resources matching the given name as a
//	 * Vector of Resource objects.  The Vector will contain only one
//	 * entry for non-partitioned resources.
//	 * 
//	 * @param name
//	 * @return
//	 */
//	public Vector<Resource> getResource( String name ) {
//		return resources.get( name );
//	} // getResource
//
//	/**
//	 * Returns a Vector containing the names of all of the nodes
//	 * a resource is partitioned across.  Used for multi-queries.
//	 * If a resource is not partitioned, this will return just the
//	 * single node name it lives on.
//	 * 
//	 * @param name
//	 * @return
//	 */
//	public Vector<String> getNodeNamesForResource( String name ) {
//		Vector<Resource> list = resources.get( name );
//		if( list == null ) {
//			return null;
//		} // if
//		
//		Vector<String> out = new Vector<String>();
//		for( Resource res : list ) {
//			out.add( res.getNode() );
//		} // for
//		return out;
//	} // getNodeNamesForResource
//
//	/**
//	 * Return the Partition Index object for the given resource and column name.
//	 * 
//	 */
//	public PartitionIndex getPartitionIndex( String resource, String columnName ) {
//		return partitionIndexes.get( PartitionIndex.makeKey( resource, columnName ) );
//	} // getPartitionIndex
//	
//	/**
//	 * Returns the primary partition index for the given resource.
//	 * 
//	 * @param resource
//	 * @return
//	 */
//	public PartitionIndex getPrimaryPartitionIndex( String resource ) {
//		return primaryIndexes.get( resource );
//	} // getPrimaryPartitionIndex
//
//	/**
//	 * INTERNAL USE ONLY-- returns the node list for this hive.
//	 * 
//	 * @return
//	 */
//	public HashMap<String,PartitionIndex> getPartitionIndexes() {
//		return partitionIndexes;
//	}
//
//	/**
//	 * INTERNAL USE ONLY-- sets the list of partition indexes.
//	 * @param indexes
//	 */
//	public void setPartitionIndexes( HashMap<String, PartitionIndex> indexes ) {
//		this.partitionIndexes = indexes;
//	} // setPartitionIndexes
//
//	public org.hivedb.Connection getDBConnection( String nodeName, int readWriteMode ) throws HiveReadOnlyException, HiveException {
//		Node node = getNode( nodeName );
//		
//		if( readWriteMode == Resource.ACCESS_WRITE && node.isReadOnly() ) {
//			throw new HiveReadOnlyException( "Cannot write, node '" + nodeName + "' is in read-only state.", HiveReadOnlyException.UKNOWN_EXPECTED_DOWNTIME );
//		} // if
//
//		return getConnectionForNode( nodeName, readWriteMode );
//	} // getDBConnection
//
//	/**
//	 * Notifies the hive of the addition of a new row to a table.  this is primarily
//	 * important for partitioned tables, but as long as you make a habit of always
//	 * calling it for all inserts, your tables can be migrated transparently without
//	 * changing the application.  If the table is not partitioned, this is a no-op.
//	 * Use this for any column on which you want fast look-ups.  ALWAYS insert the
//	 * row's primary key before inserting secondary keys.
//	 * 
//	 * @param conn -- the connection used to insert the object itself
//	 * @param resourceName
//	 * @param columnKey
//	 * @param value
//	 */
//	public void insertPrimaryKey( org.hivedb.Connection conn, String resourceName, String columnKey, Object value ) throws SQLException, HiveException, HiveReadOnlyException {
//		PartitionIndex pi = this.getPartitionIndex( resourceName, columnKey );
//		if( pi == null ) {
//			return;
//		} // if
//		pi.insertPrimaryKey( conn.getNodeName(), value );
//	} // insertPrimaryKey
//	
//	/**
//	 * Notifies the hive of the addition of a new row to a table.  this is primarily
//	 * important for partitioned tables, but as long as you make a habit of always
//	 * calling it for all inserts, your tables can be migrated transparently without
//	 * changing the application.  If the table is not partitioned, this is a no-op.
//	 * Use this for any column on which you want fast look-ups.  ALWAYS insert the
//	 * row's primary key before inserting secondary keys.
//	 * 
//	 * @param resourceName
//	 * @param columnKey
//	 * @param value
//	 */
//	public void insertSecondaryKey( String resourceName, Object primaryKeyValue, String columnKey, Object myValue ) throws SQLException, HiveException, HiveReadOnlyException {
//		PartitionIndex pi = this.getPartitionIndex( resourceName, columnKey );
//		if( pi == null ) {
//			return;
//		} // if
//		pi.insertSecondaryKey( primaryKeyValue, myValue );
//	} // insertSecondaryKey
//	
//
//	/**
//	 * Notifies the hive of a change to a fast-index key for an existing
//	 * row in a table.  this is primarily important for partitioned tables,
//	 * but as long as you make a habit of always calling it for all inserts, your
//	 * tables can be migrated transparently without changing the application.
//	 * If the table is not partitioned, this is a no-op.
//	 * 
//	 * @param resourceName
//	 * @param columnKey
//	 * @param value
//	 */
//	public void updateSecondaryKey( String resourceName, Object primaryKeyValue, String columnKey, Object newValue ) throws SQLException, HiveException, HiveReadOnlyException {
//		PartitionIndex pi = this.getPartitionIndex( resourceName, columnKey );
//		if( pi == null ) {
//			return;
//		} // if
//		pi.updateSecondaryKey( primaryKeyValue, newValue );
//	} // updateSecondaryKey
//
//	/**
//	 * Removes a key/value from the hive's indexing mechanism.  If the resource is
//	 * not a partitioned table, this is a no-op.
//	 */
//	public void removeKey( String resourceName, String columnKey, Object value ) throws SQLException, HiveException, HiveReadOnlyException {
//		PartitionIndex pi = this.getPartitionIndex( resourceName, columnKey );
//		if( pi == null || pi.getLevel() == PartitionIndex.LEVEL_LINKED ) { // linked entries dont actually have their own table.
//			return;
//		} // if
//		
//		pi.removeKey( value );
//	} // removeKey
//	
//	/**
//	 * Caller describes what db table is of interest and other selection criteria, and the
//	 * hive determines which Connection pool to draw from.
//	 * @param resource
//	 * @param readWriteMode
//	 * @param columnKey
//	 * @param value
//	 * @return
//	 */
//	public org.hivedb.Connection getDBConnection( String resource, int readWriteMode, String columnKey, Object value ) throws SQLException, HiveReadOnlyException, HiveException {
//		// get the list of resources represented by this id...
//		Vector<Resource> list = getResource( resource );
//		
//		// make a list of the resources with a matching read/write mode...
//		Vector<Resource> matches = new Vector<Resource>();
//		if( readWriteMode != Resource.ACCESS_READ && readWriteMode != Resource.ACCESS_WRITE ) {
//			throw new HiveException( "Invalid readWriteMode." );
//		} // if
//
//		if( readWriteMode == Resource.ACCESS_WRITE && isReadOnly() ) {
//			throw new HiveReadOnlyException( "Cannot write, hive is in read-only state.", HiveReadOnlyException.UKNOWN_EXPECTED_DOWNTIME );
//		} // if
//
//		if( list == null || list.size() == 0 ) {
//			throw new HiveException( "Invalid resource '" + resource + "'" );
//		} // if
//		for( Resource res : list ) {
//			if( readWriteMode == Resource.ACCESS_READ ) {
//				if( res.getType() == Resource.ACCESS_READ || res.getType() == Resource.ACCESS_READWRITE ) {
//					matches.add( res );
//				} // if
//			} else {
//				if( res.getType() == Resource.ACCESS_WRITE || res.getType() == Resource.ACCESS_READWRITE ) {
//					matches.add( res );
//				} // if
//			} // if-else
//		} // for
//		
//		if( matches.size() == 0 ) {
//			throw new HiveException( "No resources '" + resource + "' available in " + Resource.getTypeString( readWriteMode ) + "' available." );
//		} // if
//		if( list.size() == 1 ) { // only found one hit... no further processing required.
//			Resource res = list.elementAt( 0 );
//			return getConnectionForNode( res.getNode(), readWriteMode );
//		} // if
//		
//		if( columnKey != null && value != null ) { // look up partitioned resource by key
//			PartitionIndex pi = getPartitionIndex( resource , columnKey );
//			String nodeName = null;
//			PartitionedObjectStatus status = null;
//			if( pi.getLevel() == PartitionIndex.LEVEL_PRIMARY ) { // single-lookup... nice and easy
//				status = pi.getNodeNameForPrimaryID( value );
//			} else if( pi.getLevel() == PartitionIndex.LEVEL_SECONDARY ) { // look up secondary to find primary.
//				PartitionIndex primary = this.getPrimaryPartitionIndex( resource );
//				if( primary.getNode().equals( pi.getNode() ) ) {  // can do a simple JOIN...
//					status = PartitionIndex.getJoinedNodeName( this, primary, pi, value );
//				} else { // have to look up the secondary and then the primary in separate queries
//					Object realID = pi.getPartitionRedirect( value );
//					status = primary.getNodeNameForPrimaryID( realID );
//				} // if-else
//			} else { // linked index.. get primary
//				pi = getPrimaryPartitionIndex( pi.getTable() );
//				status = pi.getNodeNameForPrimaryID( value );
//			} // if-else
//			
//			if( status.isReadOnly() && readWriteMode == Resource.ACCESS_WRITE ) {
//				throw new HiveException( "Object is in read-only mode, writing disallowed." );
//			} // if
//			nodeName = status.getNodeName();
//			Node node = getNode( nodeName );
//			
//			if( readWriteMode == Resource.ACCESS_WRITE && node.isReadOnly() ) {
//				throw new HiveReadOnlyException( "Cannot write, node '" + nodeName + "' is in read-only state.", HiveReadOnlyException.UKNOWN_EXPECTED_DOWNTIME );
//			} // if
//
//			return getConnectionForNode( nodeName, readWriteMode );
//		
//		} else if( readWriteMode == Resource.ACCESS_READ ) {
//			throw new HiveException( "Ambiguous connection request; no selection criteria passed for read of partitioned table." );
//		} else { // we're writing to a partitioned table.  run selection mechanism to decide which one to write to.
//			String nodeName = selectResourcePartition( resource );
//
//			return getConnectionForNode( nodeName, readWriteMode );
//		} // if-else
//	} // getDBConnection
//	
//	protected org.hivedb.Connection getConnectionForNode( String nodeName, int readWriteMode ) throws HiveException {
//		Node node = getNode( nodeName );
//		org.hivedb.Connection conn = null;
//		Vector<org.hivedb.Connection> inUseList = connectionsInUse.get( node );
//		synchronized( connectionsInUse ) {
//			if( inUseList == null ) {
//				inUseList = new Vector<org.hivedb.Connection>();
//				connectionsInUse.put( node, inUseList );
//			} // if
//		} // synchronized
//
//		Stack<org.hivedb.Connection> stack = connectionsAvailable.get( node );
//		if( stack != null ) {
//			if( !stack.isEmpty() ) {
//				conn = stack.pop();
//			} // if
//			if( conn != null ) {
//				inUseList.add( conn );
//				try {
//					conn.clearWarnings();
//					conn.setAutoCommit( true );
//				} catch( Exception e ) {
//					throw new HiveException( e.getMessage() );
//				} // try-catch
//				return conn;
//			} // if-else
//		} else {
//			stack = new Stack<org.hivedb.Connection>();
//			connectionsAvailable.put( node, stack );
//		} // if-else
//		
//		try {
//			conn = new org.hivedb.Connection( node.getName(), node.getURI(), readWriteMode );
//		} catch( Exception e ) {
//			throw new HiveException( "Unable to create database connection to '" + nodeName + "' -- uname/password may be invalid." );
//		} // try-catch
//		inUseList.add( conn );
//
//		return conn;
//	} // getConnectionForNode
//
//	/**
//	 * Close the hive down, freeing all associated resources.
//	 * 
//	 */
//	public void close() {
//		Resource.freeAll( this );
//		Node.freeAll( this );
//		PartitionIndex.freeAll( this );
//		synchronized( connectionsAvailable ) {
//			for( Stack<org.hivedb.Connection> stack : connectionsAvailable.values() ) {
//				while( !stack.isEmpty() ) {
//					org.hivedb.Connection conn = stack.pop();
//					try {
//						conn.reallyClose();
//					} catch( SQLException e ) {
//					} // try-catch
//				} // while
//			} // for
//		} // synchronized
//		connectionsAvailable.clear();
//		
//		synchronized( connectionsInUse ) {
//			for( Vector<org.hivedb.Connection> list : connectionsInUse.values() ) {
//				for( org.hivedb.Connection conn : list ) {
//					try {
//						conn.reallyClose();
//					} catch( SQLException e ) {
//					} // try-catch
//				} // for
//			} // for
//		} // synchronized
//		connectionsInUse.clear();
//		
//		hives.remove( getName() );
//	} // close
//
//	protected String                    name;
//	protected boolean                   readOnly = false;
//	protected long                      revisionNumber;
//	protected java.sql.Connection       conn;
//	protected HashMap<String,Node>    nodes = null;
//	protected HashMap<String, Vector<Resource>>   resources;
//	protected HashMap<String, String[]> readShareMap;
//	protected HashMap<String, String[]> writeShareMap;
//	protected HashMap<String, PartitionIndex> partitionIndexes;
//	protected HashMap<String, PartitionIndex> primaryIndexes;
//	protected HashMap<Node, Stack<org.hivedb.Connection>>   connectionsAvailable = new HashMap<Node, Stack<org.hivedb.Connection>>();
//	protected HashMap<Node, Vector<org.hivedb.Connection>>  connectionsInUse = new HashMap<Node, Vector<org.hivedb.Connection>>();
//	protected int                                  resourceCount = 0;
//
//	protected static HashMap<String,Hive> hives = new HashMap<String,Hive>();
//
//	static {
//		// TODO: SPAWN METADATA REFRESH HEARTBEAT THREAD, WAKING UP EVERY SECOND
//	} // static initializer
//
//} // class Hive
