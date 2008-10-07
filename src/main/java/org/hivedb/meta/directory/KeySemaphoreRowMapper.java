package org.hivedb.meta.directory;

import org.hivedb.Lockable;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

@SuppressWarnings("unchecked")
public class KeySemaphoreRowMapper implements ParameterizedRowMapper {
  public Object mapRow(ResultSet rs, int arg1) throws SQLException {
    return new KeySemaphoreImpl(rs.getObject("id"), rs.getInt("node"), resolveStatus(rs));
  }

  private Lockable.Status resolveStatus(ResultSet rs) throws SQLException {
    return Lockable.Status.getByValue(rs.getInt("status"));
  }

}

