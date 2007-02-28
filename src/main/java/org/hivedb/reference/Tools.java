///**
// * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
// * data storage systems.
// * 
// * @author Kevin Kelm (kkelm@fortress-consulting.com)
// */
//package org.hivedb.reference;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Properties;
//import java.util.Vector;
//
//import org.hivedb.HiveException;
//import org.hivedb.reference.Hive;
//import org.hivedb.reference.Node;
//import org.hivedb.reference.PartitionIndex;
//import org.hivedb.reference.Resource;
//
//public class Tools {
//
//	public static final String DRIVER_LONG         = "--driver";
//	public static final String DRIVER_SHORT        = "-d";
//	public static final String CONNECT_LONG        = "--connect";
//	public static final String CONNECT_SHORT       = "-c";
//	public static final String CREATE_LONG         = "--create";
//	public static final String INSTALL_LONG        = "--install";
//	public static final String HELP_LONG           = "--help";
//	public static final String HIVE_LONG           = "--hive";
//	public static final String HIVE_SHORT          = "-h";
//	public static final String SHOW_LONG           = "--show";
//	public static final String ADDNODE_LONG        = "--addnode";
//	public static final String DELETENODE_LONG     = "--deletenode";
//	public static final String ADDRESOURCE_LONG    = "--addresource";
//	public static final String DELETERESOURCE_LONG = "--deleteresource";
//	public static final String SETREADONLY_LONG    = "--setreadonly";
//	public static final String SETSHARELEVELS_LONG = "--setsharelevels";
//	public static final String ADDPARTITIONINDEX_LONG = "--addpartitionindex";
//	public static final String DELETEPARTITIONINDEX_LONG = "--deletepartitionindex";
//	public static final String SETNODEURI_LONG     = "--setnodeuri";
//	
//	public static final String UNIX_HIVE_PROPERTY_FILE    = "~/.hivedb";
//	public static final String WINDOWS_HIVE_PROPERTY_FILE = "hivedb.txt";
////	public static final String MAC_HIVE_PROPERTY_FILE = ???;
//	public static final String PROPERTY_CONNECTION = "connect";
//	public static final String PROPERTY_DRIVER     = "driver";
//	public static final String PROPERTY_HIVE       = "hive";
//
//	public static final String DEFAULT_DRIVER      = "com.mysql.jdbc.Driver";
//	/**
//	 * Main entry point into HiveDB administrative command line tool.
//	 * @param args
//	 */
//	public static void main(String[] args) {
//		int argnum = 0;
//		
//		// Read properties file.
//	    Properties properties = null;
//	    try {
//	    	properties = new Properties();
//	        properties.load(new FileInputStream( UNIX_HIVE_PROPERTY_FILE ));
//	    } catch (IOException e) {
//	    	String path = System.getProperty("user.home") + File.separator + WINDOWS_HIVE_PROPERTY_FILE;
//	    	try {
//		        properties.load(new FileInputStream( path ) );
//	    	} catch( IOException e2 ) {
//		    	properties = null;
//	    	} // try-catch
//	    } // try-catch
//	    
//	    if( properties != null ) {
//	    	String tmp = properties.getProperty( PROPERTY_DRIVER );
//	        if( tmp != null && !tmp.equals("") ) {
//	        	dbDriver = tmp;
//	        } // if
//	        tmp = properties.getProperty( PROPERTY_CONNECTION );
//	        if( tmp != null && !tmp.equals("") ) {
////				closeConnection();
//	        	dbURI = tmp;
////				openConnection();
//	        } // if
//	        tmp = properties.getProperty( PROPERTY_HIVE );
//	        if( tmp != null && !tmp.equals("") ) {
//				hiveName = tmp;
////				closeConnection();
////				openConnection();
//				loadHive();
//	        } // if	        
//	    } // if
//		
//		if( args.length == 0 ) {
//			showHelp();
//			System.exit( 0 );
//		} // if
//		
//		while( argnum < args.length ) {
//			String cmd;
//			String parm;
//			String arg = args[argnum++];
//			if( arg.startsWith( "--" )) {
//				int offset = arg.indexOf( "=" );
//				if( offset == -1 ) {
//					cmd = arg;
//					parm = null;
//				} else {
//					cmd = arg.substring( 0, offset );
//					parm = arg.substring( offset + 1 );
//				} // if-else
//			} else {
//				cmd = arg;
//				parm = null;
//			} // if-else
//			
//			if( parm == null ) {
//				if( argnum < args.length ) {
//					parm = args[argnum];
//				} else {
//					parm = "";
//				} // if-else
//			} // if
//			
//			if( cmd.equals( HELP_LONG ) ) {
//				showHelp();
//			} else if( cmd.equals( DRIVER_LONG ) ) {
//				dbDriver = parm;
//				System.out.println("DB Driver class overridden to '" + dbDriver + "'" );
//			} else if( cmd.equals( DRIVER_SHORT ) ) {
//				dbDriver = parm;
//				argnum++;
//				System.out.println("DB Driver class overridden to '" + dbDriver + "'" );
//			} else if( cmd.equals( CONNECT_LONG ) ) {
//				closeConnection();
//				dbURI = parm;
//				openConnection();
//			} else if( cmd.equals( CONNECT_SHORT ) ) {
//				closeConnection();
//				dbURI = parm;
//				openConnection();
//				argnum++;
//			} else if( cmd.equals( HIVE_LONG ) ) {
//				hiveName = parm;
//				closeConnection();
//				openConnection();
//				loadHive();
//			} else if( cmd.equals( HIVE_SHORT ) ) {
//				hiveName = parm;
//				closeConnection();
//				openConnection();
//				loadHive();
//				argnum++;
//			} else if( cmd.equals( CREATE_LONG ) || cmd.equals( INSTALL_LONG ) ) {
//				openConnection();
//				argnum++;
//				try {
//					Hive.validateName( parm );
//					createHive( conn, cmd.equals( CREATE_LONG ), parm );
//				} catch( Exception e ) {
//					System.err.println( "FATAL: " + e.getMessage() );
//					System.exit( -1 );
//				} // try-catch				
//
//			} else if( cmd.equals( SHOW_LONG ) ) {
//				openConnection();
//				loadHive();
//				showHiveData();
//
//			} else if( cmd.equals( ADDNODE_LONG ) ) {
//				openConnection();
//				loadHive();
//				addNode( parm );
//
//			} else if( cmd.equals( DELETENODE_LONG ) ) {
//				openConnection();
//				loadHive();
//				deleteNode( parm );
//
//			} else if( cmd.equals( ADDRESOURCE_LONG ) ) {
//				openConnection();
//				loadHive();
//				addResource( parm );
//
//			} else if( cmd.equals( DELETERESOURCE_LONG ) ) {
//				openConnection();
//				loadHive();
//				deleteResource( parm );
//
//			} else if( cmd.equals( SETREADONLY_LONG ) ) {
//				openConnection();
//				loadHive();
//				setReadOnly( parm );
//
//			} else if( cmd.equals( SETSHARELEVELS_LONG ) ) {
//				openConnection();
//				loadHive();
//				setShareLevels( parm );
//			
//			} else if( cmd.equals( ADDPARTITIONINDEX_LONG ) ) {
//				openConnection();
//				loadHive();
//				addPartitionIndex( parm );
//
//			} else if( cmd.equals( DELETEPARTITIONINDEX_LONG ) ) {
//				openConnection();
//				loadHive();
//				deletePartitionIndex( parm );
//
//			} else if( cmd.equals( SETNODEURI_LONG ) ) {
//				openConnection();
//				loadHive();
//				setNodeURI( parm );
//
//			} else {
//				System.err.println("FATAL: Invalid argument '" + arg + "'" );
//				System.exit( -1 );
//			} // if-else
//		} // while
//
//	} // main
//	
//	protected static void deletePartitionIndex( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length != 2 ) {
//			System.err.println( "FATAL: Invalid parameters for " + DELETEPARTITIONINDEX_LONG + ": should be " + DELETEPARTITIONINDEX_LONG + "=<resource>,<column>" );
//			System.exit( 1 );
//		} // if
//
//		String rName = parms[0];
//		String column = parms[1];
//		PartitionIndex pi = hive.getPartitionIndex( rName, column );
//		if( pi == null ) {
//			System.err.println("FATAL: unknown partition index for resource '" + rName + "', column '" + column + "' )" );
//			System.exit( 1 );
//		} // if
//
//		try {
//			hive.removePartitionIndex( pi );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		
//		if( pi.getLevel() == PartitionIndex.LEVEL_LINKED ) {
//			System.out.println( "DELETED " + pi.getLevelString() + " PARTITION INDEX FOR RESOURCE '" + rName + "' ON TABLE '" + column  + "'" );
//		} else {
//			System.out.println( "DELETED " + pi.getLevelString() + " PARTITION INDEX FOR RESOURCE '" + rName + "' ON COLUMN '" + column  + "'" );
//		} // if-else
//	} // deletePartitionIndex
//
//	/**
//	 * Re-assign the host of an existing node entity
//	 * 
//	 * @param fields
//	 */
//	protected static void addPartitionIndex( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length != 5 ) {
//			System.err.println( "FATAL: Invalid parameters for " + ADDPARTITIONINDEX_LONG + ": should be " + ADDPARTITIONINDEX_LONG + "=<resource>,<level>,<column>,<type>,<nodee|table>" );
//			System.exit( 1 );
//		} // if
//
//		PartitionIndex primary;
//		String rName = parms[0];
//		String level = parms[1];
//		String column = parms[2];
//		String type = parms[3];
//		String sName = parms[4];
//		
//		Vector<Resource> list = hive.getResource( rName );
//		if( list == null || list.size() == 0 ) {
//			System.err.println("FATAL: Invalid resource name '" + rName + "'" );
//			System.exit( 1 );
//		} // if
//		if( list.elementAt(0).getType() != Resource.TYPE_PARTITIONED_TABLE ) {
//			System.err.println("FATAL: Resource '" + rName + "' is not a partitioned table, " );
//			System.exit( 1 );
//		} // if
//		
//		PartitionIndex pi = hive.getPartitionIndex( rName, column );
//		if( pi != null ) {
//			if( pi.getLevel() == PartitionIndex.LEVEL_LINKED ) {
//				System.err.println("FATAL: Partition index already exists for resource '" + rName + "', table '" + column + "'" );
//			} else {
//				System.err.println("FATAL: Partition index already exists for resource '" + rName + "', column '" + column + "'" );
//			} // if/else
//			System.exit( 1 );
//		} // if
//		
//		pi = new PartitionIndex( hive );
//		pi.setResource( list.elementAt(0).getName() );
//		try {
//			pi.setLevel( level );
//		} catch( HiveException e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		
//		if( pi.getLevel() != PartitionIndex.LEVEL_LINKED ) {
//			Node node = hive.getNode( sName );
//			if( node == null ) {
//				System.err.println("FATAL: Invalid node name '" + sName + "'" );
//				System.exit( 1 );
//			} // if
//			pi.setNode( node.getName() );
//		} else { // linked index... node name is really table name...
//			pi.setTable( sName );
//		} // if-else
//
//		primary = hive.getPrimaryPartitionIndex( rName );
//		if( pi.getLevel() == PartitionIndex.LEVEL_SECONDARY ) {
//			if( primary == null ) {
//				System.err.println("FATAL: Cannot create secondary partition index for resource '" + rName + "' until primary index exists." );
//				System.exit( 1 );
//			} // if
//		} else if( pi.getLevel() == PartitionIndex.LEVEL_PRIMARY ){
//			if( primary != null ) {
//				System.err.println("FATAL: A primary partition index already exists for resource '" + rName + "'." );
//				System.exit( 1 );
//			} // if
//		} else { // linked
//			if( primary != null ) {
//				System.err.println("FATAL: A primary partition index exists for resource '" + rName + "'... it does not need a linked index." );
//				System.exit( 1 );
//			} // if
//			primary = hive.getPrimaryPartitionIndex( sName );
//			if( primary == null ) {
//				System.err.println("FATAL: '" + sName + "' must name another partitioned entity." );
//				System.exit( 1 );
//			} // if
//		} // if-else
//
//		pi.setColumnName( column );
//		try {
//			pi.setDataType( type );
//		} catch( HiveException e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//
//		try {
//			PartitionIndex.createIndex( hive, pi, primary );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			e.printStackTrace();
//			System.exit( 1 );
//		} // try-catch
//
//		try {
//			hive.addPartitionIndex( pi );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//
//		if( pi.getLevel() == PartitionIndex.LEVEL_LINKED ) {
//			System.out.println( "ADDED " + level + " PARTITION INDEX FOR RESOURCE '" + rName + "' ON TABLE '" + column  + "' OF TYPE '" + type + "' ON NODE '" + sName + "'" );
//		} else {
//			System.out.println( "ADDED " + level + " PARTITION INDEX FOR RESOURCE '" + rName + "' ON COLUMN '" + column  + "' OF TYPE '" + type + "' ON NODE '" + sName + "'" );
//		} // if-else
//	} // addPartitionIndex
//
//	
//	/**
//	 * Re-assign the uri of an existing node entity
//	 * 
//	 * @param fields
//	 */
//	protected static void setNodeURI( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length != 2 ) {
//			System.err.println( "FATAL: Invalid parameters for " + SETNODEURI_LONG + ": should be " + SETNODEURI_LONG + "=<node>,<uri>" );
//			System.exit( 1 );
//		} // if
//
//		String name = parms[0];
//		String uri = parms[1];
//		
//		Node node = hive.getNode( name );
//		if( node == null ) {
//			System.err.println("FATAL: Invalid node name '" + name + "'" );
//			System.exit( 1 );
//		} // if
//		try {
//			node.setURI( uri );
//		} catch( Exception e ) {
//			System.err.println("FATAL: Invalid URI '" + uri + "'" );
//			System.exit( 1 );
//		} // try-catch
//		try {
//			node.update();
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		System.out.println( "SET NODE '" + name + "' TO URI " + uri );
//	} // setNodeURI
//	
//	/**
//	 * Sets the share level on a given resource/node/port combination.
//	 * 
//	 * @param fields
//	 */
//	protected static void setShareLevels( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length != 4 ) {
//			System.err.println( "FATAL: Invalid parameters for " + SETSHARELEVELS_LONG + ": should be " + SETSHARELEVELS_LONG + "=<resource>,<node>,<read share level>,<write share level>" );
//			System.exit( 1 );
//		} // if
//
//		String rName = parms[0];
//		String sName = parms[1];
//		String readShareStr = parms[2];
//		String writeShareStr = parms[3];
//		
//		int readShareLevel = 0;
//		try {
//			readShareLevel = Integer.parseInt( readShareStr );
//		} catch( NumberFormatException nfe ) {
//			System.err.println("FATAL: Invalid read share level '" + readShareStr + "'" );
//			System.exit( 1 );
//		} // try-catch
//		if( readShareLevel < 0 || readShareLevel > Resource.MAX_SHARE_LEVEL ) {
//			System.err.println( "FATAL: Invalid read share level '" + readShareStr + "': must be 0..." + Resource.MAX_SHARE_LEVEL );
//			System.exit( 1 );
//		} // if
//
//		int writeShareLevel = 0;
//		try {
//			writeShareLevel = Integer.parseInt( writeShareStr );
//		} catch( NumberFormatException nfe ) {
//			System.err.println("FATAL: Invalid read share level '" + writeShareStr + "'" );
//			System.exit( 1 );
//		} // try-catch
//		if( writeShareLevel < 0 || writeShareLevel > Resource.MAX_SHARE_LEVEL ) {
//			System.err.println( "FATAL: Invalid read share level '" + writeShareStr + "': must be 0..." + Resource.MAX_SHARE_LEVEL );
//			System.exit( 1 );
//		} // if
//
//		
//		Vector<Resource> list = hive.getResource( rName );
//		if( list == null ) {
//			System.err.println("FATAL: Invalid resource name '" + rName + "'" );
//			System.exit( 1 );
//		} // if
//
//		Node node = hive.getNode( sName );
//		if( node == null ) {
//			System.err.println("FATAL: Invalid node name '" + sName + "'" );
//			System.exit( 1 );
//		} // if
//		for( Resource res : list ) {
//			if( res.getNode().equals( node.getName() ) ) {
//				res.setReadShareLevel( readShareLevel );
//				res.setWriteShareLevel( writeShareLevel );
//				try {
//					res.update();
//				} catch( SQLException sql ) {
//					System.err.println( "FATAL: " + sql.getMessage() );
//					System.exit( 1 );
//				} // try-catch
//				System.out.println( "SET RESOURCE '" + rName + "' ON NODE '" + node.getName() + "' TO SHARE LEVELS (R/W) " + readShareLevel + "/" + writeShareLevel );
//				return;
//			}
//		} // for
//		System.err.println( "FATAL: Cannot set share level; no resource matches the given name/node/port." );
//		System.exit( 1 );
//	} // setShareLevel
//	
//	/**
//	 * Sets (or clears) read-only mode on either the whole hive or a specific node. 
//	 * @param fields
//	 */
//	protected static void setReadOnly( String fields ) {
//		String parms[] = getArgs( fields );
//		if( parms.length != 2 ) {
//			System.err.println( "FATAL: Invalid parameters for " + SETREADONLY_LONG + ": should be " + SETREADONLY_LONG + "=<hivename|nodename>,<0|1>" );
//			System.exit( 1 );
//		} // if
//
//		String name = parms[0];
//		boolean readOnly = parms[1].equals( "1" );
//
//		if( hive.getName().equals( name ) ) { // set hive readonly
//			hive.setReadOnly( readOnly );
//			try {
//				hive.update();
//			} catch( SQLException e ) {
//				System.err.println( "FATAL: " + e.getMessage() );
//				System.exit( 1 );
//			} // try-catch
//			System.out.println( "SET ENTIRE HIVE '" + name + "' READ-ONLY = " + readOnly );
//			if( !readOnly ) {
//				System.out.println( "NOTE: Any nodes specifically set read-only are still in that mode." );
//			} // if
//			return;
//		} // if
//
//		Node node = hive.getNode( name );
//		if( node == null ) {
//			System.err.println("FATAL: Invalid node name '" + name + "'" );
//			System.exit( 1 );
//		} // if
//		node.setReadOnly( readOnly );
//		try {
//			node.update();
//		} catch( SQLException e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		System.out.println( "SET NODE '" + name + "' READ-ONLY = " + readOnly );
//
//	} // setReadOnly
//
//	/**
//	 * Add a node to the hive.  The "fields" variable is a string holding a comma-
//	 * separated list of parameters for the method.
//	 * @param fields
//	 */
//	protected static void addNode( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length != 3 ) {
//			System.err.println( "FATAL: Invalid parameters for " + ADDNODE_LONG + ": should be " + ADDNODE_LONG + "=<name>,<read only (0|1)>,<uri>" );
//			System.exit( 1 );
//		} // if
//
//		String name = parms[0];
//		boolean readOnly = parms[1].equals( "1" );
//		String uri = parms[2];
//		
//		Node node = hive.getNode( name );
//		if( node != null ) {
//			System.err.println( "FATAL: Node '" + name + "' already exists." );
//			System.exit( 1 );
//		} // if
//		
//		node = new Node( hive );
//		try {
//			node.setName( name );
//			node.setURI( uri );
//		} catch( HiveException he ) {
//			System.err.println( "FATAL: " + he.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		node.setReadOnly( readOnly );
//		
//		try {
//			hive.addNode( node );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		System.out.println( "ADDED NODE '" + name + "' AT URI " + uri + ", READ-ONLY = " + readOnly );
//
//	} // addNode
//
//	/**
//	 * Delete a node from the hive. A node cannot be deleted if one or more
//	 * Resources are using it. 
//	 * @param name
//	 */
//	protected static void deleteNode( String name ) {
//		Node node = hive.getNode( name );
//		if( node == null ) {
//			System.err.println("FATAL: Invalid node name '" + name + "'" );
//			System.exit( 1 );
//		} // if
//		Vector uses = hive.getResourcesUsingNode( name );
//		if( uses != null && uses.size() != 0 ) {
//			System.err.println( "FATAL: Node cannot be deleted; " + uses.size() + " resources are still using it." );
//			System.exit( 1 );
//		} // if
//		try {
//			hive.removeNode( node );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		System.out.println( "DELETED NODE '" + name + "'" );		
//	} // deleteNode
//
//	/**
//	 * Add a resource to the hive.  The "fields" variable is a string
//	 * holding a comma-separated list of parameters for the method.
//	 * @param fields
//	 */
//	protected static void addResource( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length != 6 ) {
//			System.err.println( "FATAL: Invalid parameters for " + ADDRESOURCE_LONG + ": should be " + ADDRESOURCE_LONG + "=<name>,<type>,<node>,<access>,<read share level>,<write share level>" );
//			System.exit( 1 );
//		} // if
//		
//		String name = parms[0];
//		String typeStr = parms[1];
//		String nodeName = parms[2];
//		String accessStr = parms[3];
//		String readShareStr = parms[4];
//		String writeShareStr = parms[5];
//		Vector<Resource> existing = hive.getResource( name );
//		if( existing != null && existing.size() > 0 ) {
//			if( existing.elementAt(0).getType() != Resource.TYPE_PARTITIONED_TABLE 
//						&& existing.elementAt(0).getType() != Resource.TYPE_PARTITIONED_MEMCACHE ) {
//				System.err.println("FATAL: The resource '" + name + "' already exists and is not a partitioned entity." );
//				System.exit( 1 );
//			} // if
//		} // if
//		Node node = hive.getNode( nodeName );
//		if( node == null ) {
//			System.err.println( "FATAL: Invalid node name '" + nodeName + "'" );
//			System.exit( 1 );
//		} // if
//		Resource res = new Resource( hive );
//		try {
//			res.setName( name );
//			res.setType( typeStr );
//			if( existing != null && existing.elementAt(0).getType() != res.getType() ) {
//				System.err.println( "FATAL: Resource '" + name + "' is already declared with type '" + existing.elementAt(0).getTypeString() + "'"  );
//				System.exit( 1 );
//			} // if
//			res.setNode( node.getName() );
//			res.setAccess( accessStr );
//			try {
//				int readShare = Integer.parseInt( readShareStr );
//				if( readShare < 0 || readShare > Resource.MAX_SHARE_LEVEL ) {
//					System.err.println( "FATAL: Invalid share level '" + readShare + "': must be 0..." + Resource.MAX_SHARE_LEVEL );
//					System.exit( 1 );
//				} // if
//				res.setReadShareLevel( readShare );
//			} catch( Exception e ) {
//				System.err.println( "FATAL: Invalid read share level '" + readShareStr + "'" );
//				System.exit( 1 );
//			} // try-catch
//			try {
//				int writeShare = Integer.parseInt( writeShareStr );
//				if( writeShare < 0 || writeShare > Resource.MAX_SHARE_LEVEL ) {
//					System.err.println( "FATAL: Invalid share level '" + writeShare + "': must be 0..." + Resource.MAX_SHARE_LEVEL );
//					System.exit( 1 );
//				} // if
//				res.setWriteShareLevel( writeShare );
//			} catch( Exception e ) {
//				System.err.println( "FATAL: Invalid read share level '" + writeShareStr + "'" );
//				System.exit( 1 );
//			} // try-catch
//			hive.addResource( res );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		System.out.println( "ADDED RESOURCE '" + name + "' OF TYPE " + typeStr
//				+ ", NODE = " + nodeName + ", ACCESS = " + accessStr
//				+ ", SHARE LEVELS (R/W) = " + readShareStr + "/" + writeShareStr );
//	} // addResource
//
//	/**
//	 * Delete a resource from the hive.  The "fields" variable is a string holding a comma-
//	 * separated list of parameters for the method.
//	 * @param fields
//	 */
//	protected static void deleteResource( String fields ) {
//		String parms[] = getArgs( fields);
//		if( parms.length < 1 || parms.length > 2 ) {
//			System.err.println( "FATAL: Invalid parameters for " + DELETERESOURCE_LONG + ": should be " + DELETERESOURCE_LONG + "=<name>[,<node>]" );
//			System.exit( 1 );
//		} // if
//
//		String name = parms[0];
//		String node = parms.length > 1 ? parms[1] : null;
//
//		Vector<Resource> list = hive.getResource( name );
//		if( list == null ) {
//			System.err.println("FATAL: Invalid resource name '" + name + "'" );
//			System.exit( 1 );
//		} // if
//		Vector<Resource> matches = new Vector<Resource>();
//		for( Resource res : list ) {
//			if( node != null && !res.getNode().equals( node ) ) {
//				continue;
//			} // if
//			matches.add( res );
//		} // for
//		if( matches.size() == 0 ) {
//			System.err.println( "FATAL: Cannot delete resource; no resource matches the given name/node." );
//			System.exit( 1 );
//		} // if
//		if( matches.size() > 1 ) {
//			System.err.println( "FATAL: Cannot delete resource, ambiguous; " + matches.size() + " resources match the given name/node/port." );
//			System.exit( 1 );
//		} // if
//		Resource res = 	matches.elementAt(0);
//		try {
//			hive.removeResource( res );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: " + e.getMessage() );
//			System.exit( 1 );
//		} // try-catch
//		System.out.println( "DELETED RESOURCE '" + name + "' ON NODE '" + res.getNode() + "'" );
//	} // deleteResource
//	
//	/**
//	 * Show details of the hive including stats, a list of nodes, and a list of resources.
//	 */
//	protected static void showHiveData() {
//		printHiveStatistics();
//		System.out.println("");
//		System.out.println("NODES (" + hive.getNodeCount() + "):");
//		System.out.println("NAME                          | READ-ONLY |       URI        ");
//		System.out.println("------------------------------+-----------+-------------------");
//		HashMap<String,Node> nodes = hive.getNodes();
//		for( Iterator<String> i = nodes.keySet().iterator(); i.hasNext(); ) {
//			String name = i.next();
//			Node node = nodes.get( name );
//			System.out.println( String.format( "%-29s | %9s | %s", name, node.isReadOnly() ? "true" : "false", node.getURI() ) );
//		} // for
//		System.out.println("------------------------------+-----------+-------------------");
//		System.out.println("");
//		System.out.println("RESOURCES (" + hive.getResourceCount() + "):");
//		System.out.println("NAME                       |       TYPE        |       NODE       |  ACCESS   | SHARE R/W");
//		System.out.println("---------------------------+-------------------+------------------+-----------+----------");
//		HashMap<String,Vector<Resource>> resources = hive.getResources();
//		for( Iterator<String> i = resources.keySet().iterator(); i.hasNext(); ) {
//			String name = i.next();
//			Vector<Resource> list = resources.get( name );
//			for( Resource rsc : list ) {
//				System.out.println( String.format( "%-26s | %17s | %16s | %8s | %s",
//						name, rsc.getTypeString(), rsc.getNode(), rsc.getAccessString(), rsc.getReadShareLevel() + "/" + rsc.getWriteShareLevel() ) );
//			} // for
//		} // for
//		System.out.println("---------------------------+-------------------+------------------+-----------+----------");
//		System.out.println("");
//		System.out.println("PARTITION INDEXES (" + hive.getPartitionIndexCount() + "):");
//		System.out.println("TYPE      |           RESOURCE          |  NODE/TABLE   |    COLUMN    |  DATATYPE  ");
//		System.out.println("----------+-----------------------------+---------------+--------------+-----------");
//		HashMap<String,PartitionIndex> pis = hive.getPartitionIndexes();
//		for( PartitionIndex pi : pis.values() ) {
//			if( pi.getLevel() == PartitionIndex.LEVEL_LINKED ) {
//				System.out.println( String.format( "%-9s | %-27s | %-13s | %-12s | %-10s", pi.getLevelString(),
//						pi.getResource(), pi.getTable(), pi.getColumnName(), "(" + pi.getDataTypeString() + ")" ) );
//			} else {
//				System.out.println( String.format( "%-9s | %-27s | %-13s | %-12s | %-10s", pi.getLevelString(),
//						pi.getResource(), pi.getNode(), pi.getColumnName(), pi.getDataTypeString() ) );
//			} // if-else
//		} // for
//		System.out.println("----------+-----------------------------+---------------+--------------+-----------");
//		System.out.println("");
//	} // showHiveData
//	
//	/**
//	 * Display help on stdout.
//	 */
//	protected static void showHelp() {
//		System.out.println("HiveDB version " + Hive.VERSION );
//		System.out.println("----------------------------------" );
//		System.out.println("");
//		System.out.println("USAGE:" );
//		System.out.println("\thivedb-admin [" + DRIVER_LONG + "=<driver class>] " + CONNECT_LONG + "=<db URI> "
//					+ "[" + HIVE_LONG + "=<name>] <command> [<command> ...]");
//		System.out.println("");
//		System.out.println("Where <command> can be:");
//		System.out.println("  " + CREATE_LONG + "=<hivename>           Create a new hive");
//		System.out.println("  " + INSTALL_LONG + "=<hivename>          Add hive infrastructure to existing database");
//		System.out.println("  " + SHOW_LONG + "                        Display hive information");
//		System.out.println("  " + ADDNODE_LONG + "=<name>,<read only (0|1)>,<uri>");
//		System.out.println("  " + DELETENODE_LONG + "=<name>" );
//		System.out.println("  " + ADDRESOURCE_LONG + "=<name>,<type>,<node>,<access>,<read share level>,<write share level>" );
//		System.out.println("    where:");
//		System.out.println("      <type>        = TABLE|PARTITIONED_TABLE|MEMCACHE|PARTITIONED_MEMCACHE");
//		System.out.println("      <access>      = READ|WRITE|READWRITE");
//		System.out.println("      <share level> = used for partition write selection");
//		System.out.println("  " + DELETERESOURCE_LONG + "=<name>[,<node>" ); 
//		System.out.println("  " + SETREADONLY_LONG + "=<hivename|nodename>,<0|1>" );
//		System.out.println("  " + SETSHARELEVELS_LONG + "=<resource>,<node>,<read share level>,<write share level>" );
//		System.out.println("  " + ADDPARTITIONINDEX_LONG + "=<resource>,<level>,<column|table>,<type>,<node|table>" );
//		System.out.println("    where:");
//		System.out.println("      <level>       = PRIMARY|SECONDARY|LINKED");
//		System.out.println("      <column>      = column name");
//		System.out.println("      <type>        = INT|LONG|FLOAT|DOUBLE|STRING|DATE");
//		System.out.println("      <node|table>  = node name for primary/secondary, linked table name for linked.");
//		System.out.println("  " + DELETEPARTITIONINDEX_LONG + "=<resource>,<column>" );
//		System.out.println("----------------------------------" );
//		System.out.println("");
//		System.out.println("If driver is omitted, " + DEFAULT_DRIVER + " is assumed.");
//		System.out.println("*NIX: If " + UNIX_HIVE_PROPERTY_FILE + " contains these lines: " + PROPERTY_CONNECTION + "=<...>, " + PROPERTY_DRIVER + "=<...>, and/or " + PROPERTY_HIVE + "=<...>, they will be used as defaults for all commands unless explicitly overridden.    Make sure this file is set rw-------!");
//		System.out.println("WINDOWS: Same as above, but the file goes in the user's home directory and is called " + WINDOWS_HIVE_PROPERTY_FILE + ".");
//		System.out.println("");
//		
//	} // showHelp
//
//	/**
//	 * Make sure the database is closed.
//	 * 
//	 */
//	protected static void closeConnection() {
//		if( conn == null ) {
//			return;
//		} // if
//
//		try {
//			conn.close();
//		} catch( Exception e ) {
//			System.err.println( "FATAL: Database created but could not close connection.");
//			System.exit( -1 );
//		} // try-catch
//		
//		conn = null;
//	} // closeConnection
//
//	/**
//	 * Make sure the database is open.
//	 */
//	protected static void openConnection() {
//		if( conn != null ) {
//			return;
//		} // if
//
//		try {
//			Class.forName( dbDriver );
//		} catch( Exception e ) {
//			System.err.println( "FATAL: Error accessing database driver class!");
//			System.err.println( "Specific error: " + e.getMessage() );
//			System.exit( -1 );
//		} // try-catch
//
//
//		try {
//			conn = DriverManager.getConnection( dbURI );
//		} catch( Exception e ) {
//			if( dbURI == null || dbURI.equals("") ) {
//				System.err.println( "FATAL: Could not access database-- connection URI not provided.");
//			} else {
//				System.err.println( "FATAL: Could not access database-- connection URI may be incorrect.");
//			} // if-else
//			System.err.println( "REASON: " + e.getMessage() );
//			System.exit( -1 );
//		} // try-catch
//		
//	} // openConnection
//
//	/**
//	 * Create a new hive in the database pointed to by conn.  "name" must be the same as the name of the
//	 * database itself.
//	 * 
//	 * @param conn
//	 * @param name
//	 * @throws SQLException
//	 */
//	public static void createHive( java.sql.Connection conn, boolean createDB, String name ) throws SQLException {
//		java.sql.Statement stmt = null;
//		String sql = null;
//		if( createDB ) {
//			System.out.print( "Creating database..." );
//			sql = "CREATE DATABASE " + name;
//			stmt = conn.createStatement();
//			stmt.execute( sql );
//			System.out.println("DONE");
//			try {
//				stmt.close();
//				conn.close();
//			} catch( Exception e ) {
//				System.err.println( "FATAL: Database created but could not close connection.");
//				System.exit( -1 );
//			} // try-catch
//		} // if
//
//		// modify the url to include the database name so we can reconnect and get going.
//		int offset = dbURI.indexOf("?");
//		if( offset != -1 ) { // have to pry apart auth from the rest
//			String firstPart = dbURI.substring( 0, offset );
//			String lastPart = dbURI.substring( offset );
//			if( firstPart.endsWith( "/" ) ) {
//				if( !firstPart.endsWith( name + "/" ) ) {
//					firstPart += name;
//				} // if
//			} else {
//				if( !firstPart.endsWith( name ) ) {
//					firstPart += "/" + name;
//				} // if
//			} // if-else
//			dbURI = firstPart + lastPart;
//		} else { // apparently no auth present, try tacking it on to the end.
//			if( dbURI.endsWith( "/" ) ) {
//				if( !dbURI.endsWith( name + "/" ) ) {
//					dbURI += name;
//				} // if
//			} else {
//				if( !dbURI.endsWith( name ) ) {
//					dbURI += "/" + name;
//				} // if
//			} // if-else
//		} // if-else
//
//		try {
//			conn = DriverManager.getConnection( dbURI );
//		} catch( Exception e ) {
//			if( createDB ) {
//				System.err.println( "FATAL: Could not access newly created database '" + dbURI + "'.");
//			} else {
//				System.err.println( "FATAL: Could not access database '" + dbURI + "'.");
//			} // if-else
//			System.exit( -1 );
//		} // try-catch
//
//		stmt = conn.createStatement();
//
//		System.out.print( "Creating hive metadata..." );
//		sql = "CREATE TABLE hive_metadata ( " + 
//					"name varchar(64) not null, " +
//					"read_only bool, " +
//					"revision bigint " +
//				" )";		
//		stmt.execute( sql );
//		System.out.println("DONE");
//
//		System.out.print( "Adding hive metadata..." );
//		sql = "INSERT INTO hive_metadata( name, read_only, revision ) VALUES ( ?, ?, ? )";
//		java.sql.PreparedStatement pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, name );
//		pstmt.setBoolean( 2, false );
//		pstmt.setLong( 3, 1 );
//		pstmt.execute();
//		pstmt.close();
//		System.out.println("DONE");
//		
//		System.out.print( "Creating node metadata..." );
//		sql = "CREATE TABLE node_metadata ( " +
//					"id int not null auto_increment, " +
//					"name varchar(64) not null, " +
//					"uri varchar(255) not null, " +
//					"read_only boolean default 0, " +
//					"PRIMARY KEY (id) " +
//				" )";
//		stmt.execute( sql );
//		
//		sql = "INSERT INTO node_metadata( name, uri, read_only ) values ( ?, ?, ? )";
//		pstmt = conn.prepareStatement( sql );
//		pstmt.setString( 1, Node.GLOBAL_NODE_NAME );
//		pstmt.setString( 2, dbURI );
//		pstmt.setBoolean( 3, false );
//		pstmt.execute();
//		pstmt.close();
//	
//		System.out.println("DONE");
//		
//		System.out.print( "Creating resource metadata..." );
//		sql = "CREATE TABLE resource_metadata ( " +
//					"id int not null auto_increment, " +
//					"name varchar(128) not null, " +
//					"type enum( 'TABLE', 'PARTITIONED_TABLE', 'MEMCACHE', 'PARTITIONED_MEMCACHE' ) not null, " +
//					"node varchar(64) not null, " +
//					"access enum( 'READ', 'WRITE', 'READWRITE' ) not null, " +
//					"read_share_level tinyint default 1, " +
//					"write_share_level tinyint default 1, " +
//					"PRIMARY KEY (id) " +
//				" )";
//		stmt.execute( sql );
//		System.out.println("DONE");
//
//		System.out.print( "Creating partition metadata..." );
//		sql = "CREATE TABLE partition_index_metadata ( " +
//					"id int not null auto_increment, " +
//					"resource varchar(128) not null, " +
//					"node varchar(64) not null, " +
//					"level enum( 'PRIMARY', 'SECONDARY', 'LINKED' ) not null," +
//					"column_name varchar(64) not null, " +
//					"type varchar(32) not null, " +
//					"PRIMARY KEY (id) " +
//				" )";
//		stmt.execute( sql );
//		System.out.println("DONE");
//
////		System.out.print( "Granting permissions..." );
////		sql = "GRANT ALL ON " + name + ".* TO " + dbUser;
////		stmt.execute( sql );
//
//		stmt.close();
//
//		System.out.println( "Hive '" + name + "' created." );
//
//		hiveName = name;
//		closeConnection();
//		openConnection();
//		
//		loadHive();
//		printHiveStatistics();
//	} // createHive
//	
//	/**
//	 * Make sure the hive is loaded.
//	 */
//	protected static void loadHive() {
//		if( hive != null ) {
//			return;
//		} // if
//		try {
//			hive = new Hive( conn );
//		} catch( Exception e ) {
//			System.err.println("FATAL: Could not open hive '" + hiveName + "'" );
//			System.err.println("REASON: " + e.getMessage() );
//e.printStackTrace();
//			System.exit(1);
//		} // try-catch
//	} // loadHive
//	
//	/**
//	 * Print basic stats about the hive.
//	 */
//	public static void printHiveStatistics() {
//		System.out.println( "====================================" );
//		System.out.println( "Hive: " + hive.getName() );
//		System.out.println( "Read Only: " + hive.isReadOnly() );
//		System.out.println( "Revision: " + hive.getRevisionNumber() );
//		System.out.println( "Nodes declared: " + hive.getNodeCount() );
//		System.out.println( "Resources declared: " + hive.getResourceCount() );
//		System.out.println( "Partition indexes declared: " + hive.getPartitionIndexCount() );
//	} // printHiveStatistics
//	
//	protected static String[] getArgs( String fields ) {
//		String parms[] = fields.split( "," );
//		for( int i = 0; i < parms.length; i++ ) {
//			parms[i] = parms[i].trim();
//		} // for
//		
//		return parms;
//	} // getArgs
//	
//	protected static String dbURI;
//	protected static String dbDriver = DEFAULT_DRIVER;
//	protected static java.sql.Connection conn;
//	protected static Hive hive = null;
//	protected static String hiveName;
//}
