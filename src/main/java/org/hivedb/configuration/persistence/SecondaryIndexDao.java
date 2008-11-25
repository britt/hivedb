/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.configuration.persistence;

import org.hivedb.HiveRuntimeException;
import org.hivedb.SecondaryIndex;
import org.hivedb.SecondaryIndexImpl;
import org.hivedb.util.database.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class SecondaryIndexDao extends JdbcDaoSupport implements ConfigurationDataAccessObject<SecondaryIndex> {
  public SecondaryIndexDao(DataSource ds) {
    this.setDataSource(ds);
  }

  public SecondaryIndex create(SecondaryIndex secondaryIndex) {
    Object[] parameters = new Object[]{
      secondaryIndex.getResource().getId(),
      secondaryIndex.getColumnInfo().getName(),
      JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType())
    };

    KeyHolder generatedKey = new GeneratedKeyHolder();
    JdbcTemplate j = getJdbcTemplate();
    PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
      "INSERT INTO secondary_index_metadata (resource_id,column_name,db_type) VALUES (?,?,?)",
      new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR});
    creatorFactory.setReturnGeneratedKeys(true);

    int rows = j.update(creatorFactory.newPreparedStatementCreator(parameters), generatedKey);
    assertOneRowUpdated(rows, "Unable to create secondary index.");
    assertIdAssigned(generatedKey);
    
    secondaryIndex.setId(generatedKey.getKey().intValue());

    return secondaryIndex;
  }

  private void assertIdAssigned(KeyHolder generatedKey) {
    if (generatedKey.getKeyList().size() == 0)
      throw new HiveRuntimeException("Unable to retrieve generated id.");
  }

  private void assertOneRowUpdated(int rows, String message) {
    if (rows != 1)
      throw new HiveRuntimeException(message);
  }

  public List<SecondaryIndex> loadAll() {
    JdbcTemplate t = getJdbcTemplate();
    ArrayList<SecondaryIndex> results = new ArrayList<SecondaryIndex>();
    for (Object si : t.query("SELECT * FROM secondary_index_metadata",
      new SecondaryIndexRowMapper())) {
      results.add((SecondaryIndexImpl) si);
    }
    return results;
  }

  class SecondaryIndexRowMapper implements RowMapper {
    public Object mapRow(ResultSet rs, int index) throws SQLException {
      int jdbcType = Types.OTHER;
      jdbcType = JdbcTypeMapper.parseJdbcType(rs.getString("db_type"));

      SecondaryIndex si = new SecondaryIndexImpl(rs.getInt("id"),
        rs.getString("column_name"), jdbcType);
      return si;
    }
  }

  public SecondaryIndex update(SecondaryIndex secondaryIndex) {
    Object[] parameters;
    parameters = new Object[]{
      secondaryIndex.getResource().getId(),
      secondaryIndex.getColumnInfo().getName(),
      JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType()),
      secondaryIndex.getId()};

    KeyHolder generatedKey = new GeneratedKeyHolder();
    JdbcTemplate j = getJdbcTemplate();
    PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
      "UPDATE secondary_index_metadata SET resource_id=?,column_name=?,db_type=? WHERE id=?",
      new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER});
    creatorFactory.setReturnGeneratedKeys(true);
    int rows = j.update(creatorFactory
      .newPreparedStatementCreator(parameters), generatedKey);
    if (rows != 1)
      throw new HiveRuntimeException("Unable to update secondary index: " + secondaryIndex.getId());
    return secondaryIndex;
  }

  public void delete(SecondaryIndex secondaryIndex) {
    Object[] parameters;
    parameters = new Object[]{secondaryIndex.getId()};
    JdbcTemplate j = getJdbcTemplate();
    PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
      "DELETE FROM secondary_index_metadata WHERE id=?",
      new int[]{Types.INTEGER});
    int rows = j.update(creatorFactory
      .newPreparedStatementCreator(parameters));
    if (rows != 1)
      throw new HiveRuntimeException("Unable to delete secondary index for id: " + secondaryIndex.getId());
  }

  public List<SecondaryIndex> findByResource(int id) {
    JdbcTemplate t = getJdbcTemplate();
    ArrayList<SecondaryIndex> results = new ArrayList<SecondaryIndex>();
    for (Object si : t.query("SELECT * FROM secondary_index_metadata WHERE resource_id = ?",
      new Object[]{id},
      new SecondaryIndexRowMapper())) {
      results.add((SecondaryIndexImpl) si);
    }
    return results;
  }
}
