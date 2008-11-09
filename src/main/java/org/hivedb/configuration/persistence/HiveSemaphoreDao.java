/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.configuration.persistence;

import org.hivedb.HiveRuntimeException;
import org.hivedb.HiveSemaphore;
import org.hivedb.HiveSemaphoreImpl;
import org.hivedb.Lockable.Status;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Atom;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class HiveSemaphoreDao extends JdbcDaoSupport implements SingleEntityConfigurationDataAccessObject<HiveSemaphore> {
  public HiveSemaphoreDao(DataSource ds) {
    this.setDataSource(ds);
  }

  public HiveSemaphore get() {
    return getSemaphore();
  }

  private HiveSemaphore getSemaphore() {
    JdbcTemplate t = getJdbcTemplate();
    Collection<HiveSemaphore> results;
    try {
      results = t.query("SELECT * FROM semaphore_metadata", new HiveSemaphoreRowMapper());
    } catch (BadSqlGrammarException ex) {
      throw new HiveRuntimeException(
        "Exception loading HiveSemaphore -- verify that semaphore_metadata has one and only one row: "
          + ex.getMessage());
    }

    assertSemaphoreExists(results);
    assertHiveSemaphoreIsSingular(results.size());

    return Atom.getFirstOrThrow(results);
  }

  private void assertSemaphoreExists(Collection<HiveSemaphore> results) {
    if (results.size() == 0)
      throw new HiveSemaphoreNotFound(
        "Exception loading HiveSemaphore -- query returned no results.");
  }

  public Collection<HiveSemaphore> loadAll() {
    return Lists.newList(getSemaphore());
  }

  public HiveSemaphore create(HiveSemaphore entity) {
    JdbcTemplate j = getJdbcTemplate();
    Object[] parameters = new Object[]{entity.getStatus().getValue(), entity.getRevision()};
    try {
      j.update("INSERT INTO semaphore_metadata (status,revision) VALUES (?,?)", parameters);
    } catch (BadSqlGrammarException e) {
      throw new HiveSemaphoreNotFound(e.getMessage());
    }
    return entity;
  }

  public HiveSemaphore update(HiveSemaphore hs) {
    //Unilateral decision to abandon implicit creation
    Object[] parameters = new Object[]{hs.getStatus().getValue(),
      hs.getRevision()};
    JdbcTemplate j = getJdbcTemplate();
    try {
      int rows = j.update("UPDATE semaphore_metadata SET status = ?, revision = ?", parameters);
      assertHiveSemaphoreIsSingular(rows);
    } catch (BadSqlGrammarException e) {
      throw new HiveSemaphoreNotFound(e.getMessage());
    }
    return hs;
  }

  private void assertHiveSemaphoreIsSingular(int rows) {
    if (rows > 1)
      throw new IllegalStateException("Hive semaphore contians more than one row and has been corrupted.");
  }

  public void delete(HiveSemaphore entity) {
    throw new UnsupportedOperationException("Operation not implemented.");
  }

  public boolean doesHiveSemaphoreExist() {
    boolean exists;
    try {
      get();
      exists = true;
    } catch (HiveSemaphoreNotFound e) {
      exists = false;
    }
    return exists;
  }

  protected class HiveSemaphoreRowMapper implements RowMapper {
    public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
      return new HiveSemaphoreImpl(Status.getByValue(rs.getInt("status")), rs.getInt("revision"));
    }
  }

  public void incrementAndPersist() {
    HiveSemaphore hs = get();
    hs.incrementRevision();
    update(hs);
  }

  public class HiveSemaphoreNotFound extends HiveRuntimeException {
    private static final long serialVersionUID = 7237048097222555154L;

    public HiveSemaphoreNotFound(String msg) {
      super(msg);
    }
  }
}
