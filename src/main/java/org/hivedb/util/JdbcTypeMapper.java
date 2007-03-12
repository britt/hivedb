package org.hivedb.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

import org.hivedb.HiveDbDialect;
import org.hivedb.HiveException;
import org.hivedb.HiveRuntimeException;

public class JdbcTypeMapper {
	public static String BIGINT = "BIGINT";

	public static String CHAR = "CHAR";

	public static String DATE = "DATE";

	public static String DOUBLE = "DOUBLE";

	public static String FLOAT = "FLOAT";

	public static String INTEGER = "INTEGER";

	public static String SMALLINT = "SMALLINT";

	public static String TIMESTAMP = "TIMESTAMP";

	public static String TINYINT = "TINYINT";

	public static String VARCHAR = "VARCHAR";

	public static String jdbcTypeToString(int jdbcType) {
		switch (jdbcType) {
		case Types.BIGINT:
			return BIGINT;
		case Types.CHAR:
			return CHAR;
		case Types.DATE:
			return DATE;
		case Types.DOUBLE:
			return DOUBLE;
		case Types.FLOAT:
			return FLOAT;
		case Types.INTEGER:
			return INTEGER;
		case Types.SMALLINT:
			return SMALLINT;
		case Types.TINYINT:
			return TINYINT;
		case Types.TIMESTAMP:
			return TIMESTAMP;
		case Types.VARCHAR:
			return VARCHAR;
		}
		throw new HiveRuntimeException("No known JDBC type: " + jdbcType, null);
	}

	public static int parseJdbcType(String dbString) throws HiveException {
		String upperDbString = dbString.toUpperCase();
		if (BIGINT.equals(upperDbString))
			return Types.BIGINT;
		if (CHAR.equals(upperDbString))
			return Types.CHAR;
		if (DATE.equals(upperDbString))
			return Types.DATE;
		if (DOUBLE.equals(upperDbString))
			return Types.DOUBLE;
		if (FLOAT.equals(upperDbString))
			return Types.FLOAT;
		if (INTEGER.equals(upperDbString))
			return Types.INTEGER;
		if (SMALLINT.equals(upperDbString))
			return Types.SMALLINT;
		if (TIMESTAMP.equals(upperDbString))
			return Types.TIMESTAMP;
		if (TINYINT.equals(upperDbString))
			return Types.TINYINT;
		if (VARCHAR.equals(upperDbString))
			return Types.VARCHAR;
		throw new HiveException("No known JDBC type: " + upperDbString);
	}

	public static int primitiveTypeToJdbcType(Class<?> classType) {
		if (classType.equals(int.class) || classType.equals(Integer.class))
			return Types.INTEGER;
		if (classType.equals(long.class) || classType.equals(Long.class))
			return Types.BIGINT;
		if (classType.equals(float.class) || classType.equals(Float.class))
			return Types.FLOAT;
		if (classType.equals(double.class) || classType.equals(Double.class))
			return Types.DOUBLE;
		if (classType.equals(String.class))
			return Types.VARCHAR;
		if (classType.equals(Date.class))
			return Types.DATE;
		if (classType.equals(char.class) || classType.equals(Character.class))
			return Types.CHAR;
		throw new HiveRuntimeException("No known database type for class "
				+ classType.getCanonicalName(), null);
	}

	public static void insertJdbcTypeParameter(
			PreparedStatement preparedStatement, int index, Object value)
			throws SQLException {
		Class classType = value.getClass();
		if (classType.equals(int.class) || classType.equals(Integer.class))
			preparedStatement.setInt(index, (Integer) value);
		else if (classType.equals(long.class) || classType.equals(Long.class))
			preparedStatement.setLong(index, (Long) value);
		else if (classType.equals(float.class) || classType.equals(Float.class))
			preparedStatement.setFloat(index, (Float) value);
		else if (classType.equals(double.class)
				|| classType.equals(Double.class))
			preparedStatement.setDouble(index, (Double) value);
		else if (classType.equals(String.class))
			preparedStatement.setString(index, (String) value);
		else if (classType.equals(Date.class))
			preparedStatement.setDate(index, new java.sql.Date(((Date) value)
					.getTime()));
		else if (classType.equals(char.class)
				|| classType.equals(Character.class))
			preparedStatement.setString(index, value.toString());
		else if (classType.equals(boolean.class)
				|| classType.equals(Boolean.class))
			preparedStatement.setBoolean(index, (Boolean) value);
		else
			throw new HiveRuntimeException("No known database type for class "
					+ classType.getCanonicalName(), null);
	}

	/**
	 * This method exists instead of use resultSet.getObject() to give us more
	 * control over the resulting Java type
	 * 
	 * @param resultSet
	 * @param index
	 * @param jdbcType
	 * @return
	 * @throws HiveException
	 * @throws SQLException
	 */
	public static Object getJdbcTypeResult(ResultSet resultSet, int index,
			int jdbcType) throws SQLException {
		switch (jdbcType) {
		case Types.BIGINT:
			return resultSet.getLong(index);
		case Types.CHAR:
			String str = resultSet.getString(index);
			return (str.length() == 1) ? str.toCharArray()[0] : null;
		case Types.DATE:
			return new Date(resultSet.getDate(index).getTime());
		case Types.DOUBLE:
			return resultSet.getDouble(index);
		case Types.FLOAT:
			return resultSet.getFloat(index);
		case Types.INTEGER:
			return resultSet.getInt(index);
		case Types.SMALLINT:
			return resultSet.getShort(index);
		case Types.TIMESTAMP:
			return new Date(resultSet.getTime(index).getTime());
		case Types.VARCHAR:
			return resultSet.getString(index);
		}
		throw new HiveRuntimeException("No known JDBC type: " + jdbcType, null);
	}

	public static String jdbcTypeToSqlTypeString(int jdbcType,
			HiveDbDialect hiveDbDialect) {
		if (hiveDbDialect.equals(HiveDbDialect.MySql)) {
			switch (jdbcType) {
			case Types.BIGINT:
				return "bigint(20) unsigned";
			case Types.INTEGER:
				return "int(10) unsigned";
			case Types.DATE:
				return "datetime";
			default:
				throw new RuntimeException("Type " + jdbcType
						+ " not supported. Add it here");
			}
		} else if (hiveDbDialect.equals(HiveDbDialect.Derby)) {
			switch (jdbcType) {
			case Types.BIGINT:
			case Types.INTEGER:
				return "int";
			case Types.DATE:
				return "datetime";
			default:
				throw new RuntimeException("Type " + jdbcType
						+ " not supported. Add it here");
			}
		} else
			throw new RuntimeException("Unsupported database dialect: "
					+ hiveDbDialect.toString());
	}
}
