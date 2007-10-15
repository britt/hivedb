package org.hivedb.util.database;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;

/***
 * Common RowMapper implementations used throughout HiveDB
 * @author bcrawford
 *
 */
public class RowMappers {
	
	public static RowMapper newIntegerRowMapper() {
		return new IntegerRowMapper();
	}
	
	public static RowMapper newBooleanRowMapper() {
		return new BooleanRowMapper();
	}
	
	public static ParameterizedRowMapper newObjectRowMapper(int type) {
		return new ObjectRowMapper(type);
	}
	
	public static RowMapper newTrueRowMapper() {
		return new TrueRowMapper();
	}
	
	static class IntegerRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return rs.getInt(1);		
		}
	}
	static class BooleanRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return rs.getBoolean(1);		
		}
	}
	static class ObjectRowMapper implements ParameterizedRowMapper {
		int jdbcType;
		public ObjectRowMapper(int jdbcType)
		{
			this.jdbcType = jdbcType;
		}
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return JdbcTypeMapper.getJdbcTypeResult(rs, 1, jdbcType);		
		}
	}
	static class TrueRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return true;
		}
	}
}
