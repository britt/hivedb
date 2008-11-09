/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.configuration.persistence;

import org.hivedb.HiveRuntimeException;
import org.hivedb.Resource;
import org.hivedb.ResourceImpl;
import org.hivedb.SecondaryIndex;
import org.hivedb.util.database.JdbcTypeMapper;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class ResourceDao extends JdbcDaoSupport implements ConfigurationDataAccessObject<Resource> {
	private DataSource ds;
  private SecondaryIndexDao secondaryIndexDao;

  public ResourceDao(DataSource ds) {
		this.ds = ds;
		this.setDataSource(ds);
    secondaryIndexDao = new SecondaryIndexDao(ds);    
  }

	public Collection<Resource> loadAll() {
    Collection<Resource> results =
      (Collection<Resource>) getJdbcTemplate().query("SELECT * FROM resource_metadata",new ResourceRowMapper());
		return results;
	}

	public Resource create(Resource newResource) {
    int columnType = newResource.getIdIndex().getColumnInfo().getColumnType();
    Object[] parameters = new Object[] { newResource.getName(), JdbcTypeMapper.jdbcTypeToString(columnType),newResource.isPartitioningResource()};

    KeyHolder generatedKey = new GeneratedKeyHolder();
    int rows = update(
      "INSERT INTO resource_metadata (name,db_type,is_partitioning_resource) VALUES (?,?,?)",
      parameters,
      new int[] {Types.VARCHAR,Types.VARCHAR,Types.BIT},
      generatedKey);

    assertOneRowModified(rows,"Unable to create Resource.");
    assertIdAssigned(generatedKey);

    newResource.setId(generatedKey.getKey().intValue());
		
		for (SecondaryIndex si : newResource.getSecondaryIndexes()) {
			secondaryIndexDao.create(si);
    }

    return newResource;
	}

  public Resource update(Resource resource) {
    int columnType = resource.getIdIndex().getColumnInfo().getColumnType();
    Object[] parameters = new Object[] { resource.getName(), JdbcTypeMapper.jdbcTypeToString(columnType),resource.isPartitioningResource(), resource.getId()};

    KeyHolder generatedKey = new GeneratedKeyHolder();

    int rows = update(
      "UPDATE resource_metadata SET name=?,db_type=?,is_partitioning_resource=? WHERE id=?",
      parameters,
      new int[] {Types.VARCHAR,Types.VARCHAR,Types.BIT,Types.INTEGER},
      generatedKey
    );

    assertOneRowModified(rows, "Unable to update Resource.");

    for (SecondaryIndex si : resource.getSecondaryIndexes()) {
      secondaryIndexDao.update(si);
    }

    return resource;
  }
	
	public void delete(Resource resource) {
		for (SecondaryIndex si : resource.getSecondaryIndexes()) {
			secondaryIndexDao.delete(si);
    }

    int rows = update(
      "DELETE FROM resource_metadata WHERE id=?",
      new Object[] { resource.getId()},
      new int[] { Types.INTEGER },
      new GeneratedKeyHolder()
    );
		assertOneRowModified(rows, "Unable to delete resource");
	}
  
  private int update(String sql, Object[] parameters, int[] types, KeyHolder generatedKey) {
    PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(sql,types);
    creatorFactory.setReturnGeneratedKeys(true);
    int rows = getJdbcTemplate().update(creatorFactory.newPreparedStatementCreator(parameters), generatedKey);
    return rows;
  }

  private void assertIdAssigned(KeyHolder generatedKey) {
    if (generatedKey.getKeyList().size() == 0)
			throw new HiveRuntimeException("Unable to retrieve generated id.");
  }

  private void assertOneRowModified(int rows, String message) {
    if (rows != 1)
			throw new HiveRuntimeException(message);
  }

  protected class ResourceRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			SecondaryIndexDao sDao = new SecondaryIndexDao(ds);
			List<SecondaryIndex> indexes = sDao.findByResource(rs.getInt("id"));
			return new ResourceImpl(rs.getInt("id"), rs.getString("name"), JdbcTypeMapper.parseJdbcType(rs.getString("db_type")), rs.getBoolean("is_partitioning_resource"), indexes);
		}
	}

}
