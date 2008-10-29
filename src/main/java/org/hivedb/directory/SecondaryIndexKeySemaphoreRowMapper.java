package org.hivedb.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SecondaryIndexKeySemaphoreRowMapper extends ResourceKeySemaphoreRowMapper {
  private final static Log log = LogFactory.getLog(SecondaryIndexKeySemaphoreRowMapper.class);

  @Override
  public Object mapRow(ResultSet rs, int arg1) throws SQLException {
    return new SecondaryIndexKeySemaphoreImpl((ResourceKeySemaphore) super.mapRow(rs, arg1), rs.getObject("resourceId"));
  }
}

