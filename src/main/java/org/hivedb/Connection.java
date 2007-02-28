/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 */
package org.hivedb;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.hivedb.meta.AccessType;

public class Connection implements java.sql.Connection {
		
	public static final int MODE_READ	  = 0;
	public static final int MODE_WRITE	 = 1;
	public static final int MODE_READWRITE = 2;
   
	public Connection( String nodeName, java.sql.Connection conn ) {
		this.nodeName = nodeName;
		this.delegate = conn;
	} // constructor
	
	public Connection( String nodeName, String dbURI, AccessType readWriteMode ) throws SQLException{
		this.nodeName = nodeName;
	this.dbURI = dbURI;
	this.readWriteMode = readWriteMode; 
	validate();
	
	setReadOnly( readWriteMode == AccessType.Read );
	} // constructor

	public void validate() throws SQLException {
	if( lastUsed < System.currentTimeMillis() - TIMEOUT_MINUTES ) { // re-open connection
		if( delegate != null ) {
			delegate.close();
		} // if
		delegate = DriverManager.getConnection( dbURI, connectionProps );
		closeAllStatements();
	} // if

	lastUsed = System.currentTimeMillis();
	} // validate

	public String getNodeName() {
		return nodeName;
	} // getNodeName

	public void clearWarnings() throws SQLException {
		delegate.clearWarnings();
	}

	public void close() throws SQLException {
		closeAllStatements();
	}

	public void reallyClose() throws SQLException {
		closeAllStatements();
		delegate.close();
	}
	
	public void commit() throws SQLException {
		delegate.commit();
	}

	public java.sql.Statement createStatement() throws SQLException {
		return createStatement( java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY, delegate.getHoldability() );
	}

	public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return createStatement( resultSetType, resultSetConcurrency, delegate.getHoldability() );
	}

	public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {

		String key = resultSetType + ":" + resultSetConcurrency + ":" + resultSetHoldability;
		java.sql.Statement stmt = getCachedStatement( stmtsAvail, stmtsInUse, key );
		if( stmt != null ) {
			return stmt;
		} // if
		
		stmt = delegate.createStatement(resultSetType,
				resultSetConcurrency, resultSetHoldability);

		org.hivedb.Statement stmt2 = new org.hivedb.Statement();
		stmt2.setKey( key );
		encacheStatement( stmtsAvail, stmtsInUse, stmt2, true );

		return stmt2;
	}

	public boolean getAutoCommit() throws SQLException {
		return delegate.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		return delegate.getCatalog();
	}

	public int getHoldability() throws SQLException {
		return delegate.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return delegate.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		return delegate.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return delegate.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		return delegate.getWarnings();
	}

	public boolean isClosed() throws SQLException {
		return delegate.isClosed();
	}

	public boolean isReadOnly() throws SQLException {
		return delegate.isReadOnly();
	}

	public String nativeSQL(String arg0) throws SQLException {
		return delegate.nativeSQL( arg0 );
	}

	public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
		return prepareCall( sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY, delegate.getHoldability() );
	}

	public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return prepareCall( sql, resultSetType,
				resultSetConcurrency, delegate.getHoldability() );
	}

	public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {

		String key = resultSetType + ":" + resultSetConcurrency + ":" + resultSetHoldability;
		java.sql.CallableStatement stmt = (java.sql.CallableStatement)getCachedStatement( stmtsAvail, stmtsInUse, key );
		if( stmt != null ) {
			return stmt;
		} // if
		
		stmt = delegate.prepareCall( sql, resultSetType,
				resultSetConcurrency, resultSetHoldability );

		org.hivedb.CallableStatement stmt2 = new org.hivedb.CallableStatement();
		stmt2.setKey( key );
		encacheStatement( stmtsAvail, stmtsInUse, stmt2, true );

		return (java.sql.CallableStatement)stmt2;
	}

	public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
		return prepareStatement( sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY, delegate.getHoldability() );
	}

	public java.sql.PreparedStatement prepareStatement(String sql, int autoGenKeyIndex)
			throws SQLException {
		String key = sql + "--";
		org.hivedb.PreparedStatement stmt = (org.hivedb.PreparedStatement)getCachedStatement( pstmtsAvail, pstmtsInUse, key );
		if( stmt != null ) {
			return stmt;
		} // if
		java.sql.PreparedStatement stmt2 = delegate.prepareStatement( sql, autoGenKeyIndex );
		stmt = new org.hivedb.PreparedStatement( this, stmt2 );
		stmt.setKey( key );
		encacheStatement( pstmtsAvail, pstmtsInUse, stmt, true );
		return stmt;
	}

	public java.sql.PreparedStatement prepareStatement(String sql, int[] arg1)
			throws SQLException {
		
		// TODO: How to encache a statement with arg params?  should we even try?  for now, no.

		java.sql.PreparedStatement stmt2 = delegate.prepareStatement( sql, arg1 );
		org.hivedb.PreparedStatement stmt = new org.hivedb.PreparedStatement( this, stmt2 );
		return stmt;
	}

	public java.sql.PreparedStatement prepareStatement(String sql, String[] arg1)
			throws SQLException {
		String key = sql + "--";
		org.hivedb.PreparedStatement stmt = (org.hivedb.PreparedStatement)getCachedStatement( pstmtsAvail, pstmtsInUse, key );
		if( stmt != null ) {
			return stmt;
		} // if
		java.sql.PreparedStatement stmt2 = delegate.prepareStatement( sql, arg1 );
		stmt = new org.hivedb.PreparedStatement( this, stmt2 );
		stmt.setKey( key );
		encacheStatement( pstmtsAvail, pstmtsInUse, stmt, true );
		return stmt;
	}

	public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		String key = sql + "-" + resultSetType + "-" + resultSetConcurrency;
		org.hivedb.PreparedStatement stmt = (org.hivedb.PreparedStatement)getCachedStatement( pstmtsAvail, pstmtsInUse, key );
		if( stmt != null ) {
			return stmt;
		} // if

		java.sql.PreparedStatement stmt2 = delegate.prepareStatement( sql, resultSetType, resultSetConcurrency );
		stmt = new org.hivedb.PreparedStatement( this, stmt2 );
		stmt.setKey( key );
		encacheStatement( pstmtsAvail, pstmtsInUse, stmt, true );
		
		return stmt;
	}

	public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int arg3) throws SQLException {
		String key = sql + "-" + resultSetType + "-" + resultSetConcurrency + "-" + arg3;
		org.hivedb.PreparedStatement stmt = (org.hivedb.PreparedStatement)getCachedStatement( pstmtsAvail, pstmtsInUse, key );
		if( stmt != null ) {
			return stmt;
		} // if

		java.sql.PreparedStatement stmt2 = delegate.prepareStatement( sql, resultSetType, resultSetConcurrency, arg3 );
		
		stmt = new org.hivedb.PreparedStatement( this, stmt2 );
		stmt.setKey( key );
		encacheStatement( pstmtsAvail, pstmtsInUse, stmt, true );
		return stmt;
	}

	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		delegate.releaseSavepoint( arg0 );
	}

	public void rollback() throws SQLException {
		delegate.rollback();
	}

	public void rollback(Savepoint arg0) throws SQLException {
		delegate.rollback( arg0 );
	}

	public void setAutoCommit(boolean arg0) throws SQLException {
		delegate.setAutoCommit( arg0 );
	}

	public void setCatalog(String arg0) throws SQLException {
		delegate.setCatalog( arg0 );
	}

	public void setHoldability(int arg0) throws SQLException {
		delegate.setHoldability( arg0 );
	}

	public void setReadOnly(boolean arg0) throws SQLException {
		delegate.setReadOnly( arg0 );
	}

	public Savepoint setSavepoint() throws SQLException {
		return delegate.setSavepoint();
	}

	public Savepoint setSavepoint(String arg0) throws SQLException {
		return delegate.setSavepoint( arg0 );
	}

	public void setTransactionIsolation(int arg0) throws SQLException {
		delegate.setTransactionIsolation( arg0 );
	}

	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		delegate.setTypeMap( arg0 );
	}

	protected java.sql.Statement getCachedStatement( Hashtable<String, Vector<Statement>> avail, 
			Hashtable<String, Vector<Statement>> inUse, String key ) {
		Vector<Statement> list = avail.get( key );
		org.hivedb.Statement stmt = null;
		if( list == null) {
			return null;
		} // if
		synchronized( list ) {
			if( list.size() == 0) {
				return null;
			} // if
			stmt = (org.hivedb.Statement)list.remove( list.size() - 1 );
		} // synchronized
		list = inUse.get( key );
		if( list == null ) {
			list = new Vector<Statement>();
			inUse.put( key, list );
		} // if
		list.add( stmt );
		try {
			stmt.clearBatch();
			stmt.clearWarnings();
		} catch( Exception e ) {
		} // try-catch
		
		return stmt;
	} // getCachedStatement

	protected void encacheStatement( Hashtable<String, Vector<Statement>> avail, 
			Hashtable<String, Vector<Statement>> inUse, 
			org.hivedb.Statement stmt, boolean toBeUsed ) {
		Vector<Statement> list = avail.get( stmt.getKey() );
		if( list == null ) {
			list = new Vector<Statement>();
			if( !toBeUsed ) {
				avail.put( stmt.getKey(), list );
			} // if
		} // if
		list = inUse.get( stmt.getKey() );
		if( list == null ) {
			list = new Vector<Statement>();
			if( toBeUsed ) {
				inUse.put( stmt.getKey(), list );
			} // if
		} // if
		list.add( stmt );
	} // encacheStatement

	protected void releaseStatement( org.hivedb.Statement stmt ) {
		Hashtable<String, Vector<Statement>> avail = null, inUse = null;
		if( stmt.getStatementType() == org.hivedb.Statement.TYPE_STATEMENT ) {
			avail = stmtsAvail;
			inUse = stmtsInUse;
		} else if( stmt.getStatementType() == org.hivedb.Statement.TYPE_CALLABLE ) {
			avail = cstmtsAvail;
			inUse = cstmtsInUse;
		} else {
			avail = pstmtsAvail;
			inUse = pstmtsInUse;
		} // if-else
		Vector<Statement> list = inUse.get( stmt.getKey() );
		if( list == null ) { // not really in use
			return;
		} // if
		synchronized( list ) {
			if( list.contains( stmt ) ) {
				list.remove( stmt );
			} // if
		} // synchronized

		synchronized( avail ) {
			list = avail.get( stmt.getKey() );

			if( list == null ) {
				list = new Vector<Statement>();
				avail.put( stmt.getKey(), list );
			} // if
		} // synchronized
		
		list.add( stmt );
	} // releaseStatement
	
	@SuppressWarnings("unchecked")
	protected void closeAllStatements() {
		// build master list of all cached statements by key
		Vector<Vector<Statement>> allLists = new Vector<Vector<Statement>>();
		allLists.addAll( stmtsAvail.values() );
		allLists.addAll( stmtsInUse.values() );
		allLists.addAll( cstmtsAvail.values() );
		allLists.addAll( cstmtsInUse.values() );
		allLists.addAll( pstmtsAvail.values() );
		allLists.addAll( pstmtsInUse.values() );
		
		// combine all statements to one big list unaffected by iterator munging
		Vector<Statement> allStatements = new Vector<Statement>();
		for( Vector<Statement> list : allLists ) {
			allStatements.addAll( list );
		} // for
		
		// tell each of the statements to close
		for( org.hivedb.Statement stmt : allStatements ) {
			stmt.close();
		} // for
		
		stmtsAvail.clear();
		stmtsInUse.clear();
		cstmtsAvail.clear();
		cstmtsInUse.clear();
		pstmtsAvail.clear();
		pstmtsInUse.clear();
	} // closeAllStatements

	public static final int TIMEOUT_MINUTES = 60 * 60 * 1000;

	protected java.sql.Connection delegate	= null;
	protected String	nodeName	= null;
	protected Hashtable<String, Vector<Statement>> stmtsAvail  = new Hashtable<String, Vector<Statement>>();
	protected Hashtable<String, Vector<Statement>> stmtsInUse  = new Hashtable<String, Vector<Statement>>();
	protected Hashtable<String, Vector<Statement>> cstmtsAvail = new Hashtable<String, Vector<Statement>>();
	protected Hashtable<String, Vector<Statement>> cstmtsInUse = new Hashtable<String, Vector<Statement>>();
	protected Hashtable<String, Vector<Statement>> pstmtsAvail = new Hashtable<String, Vector<Statement>>();
	protected Hashtable<String, Vector<Statement>> pstmtsInUse = new Hashtable<String, Vector<Statement>>();
	protected String		  dbURI	   = null;
	protected long		lastUsed	= 0;
	protected AccessType		 readWriteMode = AccessType.ReadWrite;
	protected static Properties connectionProps = new Properties();
	
	static {
		// TODO: TBD: IS THIS AN EFFECTIVE MEANS OF KEEPING CONNECTIONS FROM GOING STALE,
		// OR DOES THE REAPER THREAD NEED TO KILL OFF MORE AGRESSIVELY?
		connectionProps.put( "autoReconnect", "true" );
		
		// TODO: spawn connection reaper thread
			
	} // static initializer
		
} // class Connection
