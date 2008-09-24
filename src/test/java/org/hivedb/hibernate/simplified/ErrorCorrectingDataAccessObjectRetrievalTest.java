package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Lockable;
import org.hivedb.hibernate.simplified.session.HiveSessionFactory;
import org.hivedb.hibernate.simplified.session.SingletonHiveSessionFactoryBuilder;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.List;

@HiveTest.Config("hive_default")
public class ErrorCorrectingDataAccessObjectRetrievalTest extends HiveTest {
  private final static Log log = LogFactory.getLog(ErrorCorrectingDataAccessObjectRetrievalTest.class);
  private HiveSessionFactory hiveSessionFactory;

  @Test
	public void shouldRetrieveAnExistingEntity() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport original = getPersistentInstance(dao);
		WeatherReport report = dao.get(original.getReportId());
		assertEquals(ReflectionTools.getDifferingFields(original, report, WeatherReport.class).toString(), original.hashCode(), report.hashCode());
  }

  @Test
  public void shouldDetectChangesInPrimaryIndexKey() throws Exception {
    ErrorCorrectingDataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport original = getPersistentInstance(dao);
    assertFalse(dao.hasPartitionDimensionKeyChanged(original));    
    String newContinent = original.getContinent().equals("Asia") ? "Australia" : "Asia";
    hive.directory().insertPrimaryIndexKey(newContinent);
    hive.directory().updatePrimaryIndexKeyOfResourceId("WeatherReport", original.getReportId(), newContinent);
    assertTrue(dao.hasPartitionDimensionKeyChanged(original));
  }

  @Test
  public void shouldIndicateThatThePartitionKeyHasNotChangedForUnsaveRecords() throws Exception {
    ErrorCorrectingDataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport original = WeatherReportImpl.generate();
    assertFalse(dao.hasPartitionDimensionKeyChanged(original));
  }

  @Test
  public void shouldDeleteTheDirectoryEntryIfTheEntityIsNotPresentOnTheDataNode() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport original = getPersistentInstance(dao);
    dao.delete(original.getReportId());
    String resourceName = getEntityHiveConfig().getEntityConfig(WeatherReport.class).getResourceName();
    getHive().directory().insertResourceId(resourceName, original.getReportId(), original.getContinent());
    assertTrue(getHive().directory().doesResourceIdExist(resourceName, original.getReportId()));
    WeatherReport report = dao.get(original.getReportId());
    assertTrue(report == null);
    assertFalse(getHive().directory().doesResourceIdExist(resourceName, original.getReportId()));
  }
  
  @Test
  public void shouldReturnNullIfTheRecordDoesNotExist() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport report = dao.get(777777);
		assertTrue(report == null);
  }

  @Test
  public void shouldReturnNullIfTheRecordExistsButHasNoDirectoryEntry() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport original = getPersistentInstance(dao);
    String resourceName = getEntityHiveConfig().getEntityConfig(WeatherReport.class).getResourceName();
    getHive().directory().deleteResourceId(resourceName, original.getReportId());
    assertFalse(dao.exists(original.getReportId()));    
    WeatherReport report = dao.get(original.getReportId());
    assertTrue(report == null);
  }

  @Test
  public void shouldOnlyWarnIfTheHiveIsLockedForWrites() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport original = getPersistentInstance(dao);
    dao.delete(original.getReportId());
    String resourceName = getEntityHiveConfig().getEntityConfig(WeatherReport.class).getResourceName();
    getHive().directory().insertResourceId(resourceName, original.getReportId(), original.getContinent());
    assertTrue(getHive().directory().doesResourceIdExist(resourceName, original.getReportId()));
    getHive().updateHiveStatus(Lockable.Status.readOnly);
    WeatherReport report = dao.get(original.getReportId());
    assertTrue(report == null);
    assertTrue(getHive().directory().doesResourceIdExist(resourceName, original.getReportId()));
  }

  private HiveSessionFactory getSessionFactory() {
    if(hiveSessionFactory==null)
      hiveSessionFactory = new SingletonHiveSessionFactoryBuilder(getHive(), (List<Class<?>>) getMappedClasses(),new SequentialShardAccessStrategy()).getSessionFactory();
    return hiveSessionFactory;
  }

  private WeatherReport getPersistentInstance(DataAccessObject<WeatherReport, Integer> dao) {
      return dao.save(WeatherReportImpl.generate());
  }
}

