package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResourceKeySemaphoreRowMapper extends KeySemaphoreRowMapper {
  private final static Log log = LogFactory.getLog(ResourceKeySemaphoreRowMapper.class);

  @Override
  public Object mapRow(ResultSet rs, int arg1) throws SQLException {
    return new ResourceKeySemaphoreImpl((KeySemaphore) super.mapRow(rs, arg1), rs.getObject("primaryIndexKey"));
  }
}

