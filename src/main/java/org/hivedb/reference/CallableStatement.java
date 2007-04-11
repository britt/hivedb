/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 */
package org.hivedb.reference;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class CallableStatement extends org.hivedb.reference.PreparedStatement {

	public CallableStatement() {};

	public CallableStatement( org.hivedb.reference.Connection conn, java.sql.CallableStatement ps ) throws SQLException {
		delegate = ps;
		_conn = conn;
		type = TYPE_PREPARED;
	} // constructor

	public Array getArray(int i) throws SQLException {
		return delegate.getArray( i );
	}

	public Array getArray(String colName) throws SQLException {
		return delegate.getArray( colName );
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return delegate.getBigDecimal( columnIndex );
	}

	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return delegate.getBigDecimal( columnName );
	}

	@SuppressWarnings("deprecation")
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		return delegate.getBigDecimal( columnIndex, scale );
	}

	public Blob getBlob(int i) throws SQLException {
		return delegate.getBlob( i );
	}

	public Blob getBlob(String colName) throws SQLException {
		return delegate.getBlob( colName );
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return delegate.getBoolean( columnIndex );
	}

	public boolean getBoolean(String columnName) throws SQLException {
		return delegate.getBoolean( columnName );
	}

	public byte getByte(int columnIndex) throws SQLException {
		return delegate.getByte( columnIndex );
	}

	public byte getByte(String columnName) throws SQLException {
		return delegate.getByte( columnName );
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return delegate.getBytes( columnIndex );
	}

	public byte[] getBytes(String columnName) throws SQLException {
		return delegate.getBytes( columnName );
	}

	public Clob getClob(int i) throws SQLException {
		return delegate.getClob( i );
	}

	public Clob getClob(String colName) throws SQLException {
		return delegate.getClob( colName );
	}

	public Date getDate(int columnIndex) throws SQLException {
		return delegate.getDate( columnIndex );
	}

	public Date getDate(String columnName) throws SQLException {
		return delegate.getDate( columnName );
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return delegate.getDate( columnIndex, cal );
	}

	public Date getDate(String columnName, Calendar cal) throws SQLException {
		return delegate.getDate( columnName, cal );
	}

	public double getDouble(int columnIndex) throws SQLException {
		return delegate.getDouble( columnIndex );
	}

	public double getDouble(String columnName) throws SQLException {
		return delegate.getDouble( columnName );
	}

	public int getFetchDirection() throws SQLException {
		return delegate.getFetchDirection();
	}

	public int getFetchSize() throws SQLException {
		return delegate.getFetchSize();
	}

	public float getFloat(int columnIndex) throws SQLException {
		return delegate.getFloat( columnIndex );
	}

	public float getFloat(String columnName) throws SQLException {
		return delegate.getFloat( columnName );
	}

	public int getInt(int columnIndex) throws SQLException {
		return delegate.getInt( columnIndex );
	}

	public int getInt(String columnName) throws SQLException {
		return delegate.getInt( columnName );
	}

	public long getLong(int columnIndex) throws SQLException {
		return delegate.getLong( columnIndex );
	}

	public long getLong(String columnName) throws SQLException {
		return delegate.getLong( columnName );
	}

	public Object getObject(int columnIndex) throws SQLException {
		return delegate.getObject( columnIndex );
	}

	public Object getObject(String columnName) throws SQLException {
		return delegate.getObject( columnName );
	}

	public Object getObject(int i, Map<String, Class<?>> map)
			throws SQLException {
		return delegate.getObject( i, map );
	}

	public Object getObject(String colName, Map<String, Class<?>> map)
			throws SQLException {
		return delegate.getObject( colName, map );
	}

	public Ref getRef(int i) throws SQLException {
		return delegate.getRef( i );
	}

	public Ref getRef(String colName) throws SQLException {
		return delegate.getRef( colName );
	}

	public short getShort(int columnIndex) throws SQLException {
		return delegate.getShort( columnIndex );
	}

	public short getShort(String columnName) throws SQLException {
		return delegate.getShort( columnName );
	}

	public String getString(int columnIndex) throws SQLException {
		return delegate.getString( columnIndex );
	}

	public String getString(String columnName) throws SQLException {
		return delegate.getString( columnName );
	}

	public Time getTime(int columnIndex) throws SQLException {
		return delegate.getTime( columnIndex );
	}

	public Time getTime(String columnName) throws SQLException {
		return delegate.getTime( columnName );
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return delegate.getTime( columnIndex, cal );
	}

	public Time getTime(String columnName, Calendar cal) throws SQLException {
		return delegate.getTime( columnName, cal );
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return delegate.getTimestamp( columnIndex );
	}

	public Timestamp getTimestamp(String columnName) throws SQLException {
		return delegate.getTimestamp( columnName );
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		return delegate.getTimestamp( columnIndex, cal );
	}

	public Timestamp getTimestamp(String columnName, Calendar cal)
			throws SQLException {
		return delegate.getTimestamp( columnName, cal );
	}

	public URL getURL(int columnIndex) throws SQLException {
		return delegate.getURL( columnIndex );
	}

	public URL getURL(String columnName) throws SQLException {
		return delegate.getURL( columnName );
	}

	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
		delegate.registerOutParameter(parameterIndex, sqlType);

	}

	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
		delegate.registerOutParameter(parameterName, sqlType);

	}

	public void registerOutParameter(int parameterIndex, int sqlType, int scale)
			throws SQLException {
		delegate.registerOutParameter(parameterIndex, sqlType, scale);

	}

	public void registerOutParameter(int paramIndex, int sqlType, String typeName)
			throws SQLException {
		delegate.registerOutParameter(paramIndex, sqlType, typeName);

	}

	public void registerOutParameter(String parameterName, int sqlType, int scale)
			throws SQLException {
		delegate.registerOutParameter(parameterName, sqlType, scale);

	}

	public void registerOutParameter(String parameterName, int sqlType, String typeName)
			throws SQLException {
		delegate.registerOutParameter(parameterName, sqlType, typeName);

	}

	public void setAsciiStream(String parameterName, InputStream x, int length)
	throws SQLException {
		delegate.setAsciiStream(parameterName, x, length);
	}

	public void setBigDecimal(String parameterName, BigDecimal x)
		throws SQLException {
		delegate.setBigDecimal(parameterName, x);
	}
	
	public void setBinaryStream(String parameterName, InputStream x, int length)
		throws SQLException {
		delegate.setBinaryStream(parameterName, x, length);	
	}
	
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		delegate.setBoolean(parameterName, x);
	}
	
	public void setByte(String parameterName, byte x) throws SQLException {
		delegate.setByte(parameterName, x);
	}
	
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		delegate.setBytes(parameterName, x);
	}
	
	public void setCharacterStream(String parameterName, Reader reader,
		int length) throws SQLException {
		delegate.setCharacterStream( parameterName, reader, length );
	}
	
	public void setDate(String parameterName, Date x) throws SQLException {
		delegate.setDate(parameterName, x);
	}
	
	public void setDate(String parameterName, Date x, Calendar cal)
		throws SQLException {
		delegate.setDate(parameterName, x, cal);
	}
	
	public void setDouble(String parameterName, double x) throws SQLException {
		delegate.setDouble(parameterName, x);
	}
	
	public void setFloat(String parameterName, float x) throws SQLException {
		delegate.setFloat(parameterName, x);
	}
	
	public void setInt(String parameterName, int x) throws SQLException {
		delegate.setInt(parameterName, x);
	}
	
	public void setLong(String parameterName, long x) throws SQLException {
		delegate.setLong(parameterName, x);
	}
	
	public void setNull(String parameterName, int sqlType) throws SQLException {
		delegate.setNull(parameterName, sqlType);
	}
	
	public void setNull(String parameterName, int sqlType, String typeName)
		throws SQLException {
		delegate.setNull(parameterName, sqlType, typeName);
	}
	
	public void setObject(String parameterName, Object x) throws SQLException {
		delegate.setObject(parameterName, x);
	}
	
	public void setObject(String parameterName, Object x, int targetSqlType)
		throws SQLException {
		delegate.setObject(parameterName, x, targetSqlType);
	}
	
	public void setObject(String parameterName, Object x, int targetSqlType,
		int scale) throws SQLException {
		delegate.setObject(parameterName, x, targetSqlType, scale);
	}
	
	public void setShort(String parameterName, short x) throws SQLException {
		delegate.setShort(parameterName, x);
	}
	
	public void setString(String parameterName, String x) throws SQLException {
		delegate.setString(parameterName, x);
	}
	
	public void setTime(String parameterName, Time x) throws SQLException {
		delegate.setTime(parameterName, x);
	}
	
	public void setTime(String parameterName, Time x, Calendar cal)
		throws SQLException {
		delegate.setTime(parameterName, x, cal);
	}
	
	public void setTimestamp(String parameterName, Timestamp x)
		throws SQLException {
		delegate.setTimestamp(parameterName, x);
	}
	
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
		throws SQLException {
		delegate.setTimestamp(parameterName, x, cal);
	}
	
	public void setURL(String parameterName, URL val) throws SQLException {
		delegate.setURL(parameterName, val);
	}
	
	public boolean wasNull() throws SQLException {
		return delegate.wasNull();
	}


	protected java.sql.CallableStatement delegate        = null;

}
