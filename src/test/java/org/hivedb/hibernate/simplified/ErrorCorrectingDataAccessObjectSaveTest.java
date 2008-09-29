package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Session;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.hibernate.simplified.session.HiveSessionFactory;
import org.hivedb.hibernate.simplified.session.HiveSessionFactoryImpl;
import org.hivedb.hibernate.simplified.session.SingletonHiveSessionFactoryBuilder;
import org.hivedb.util.Lists;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

@HiveTest.Config("hive_default")
public class ErrorCorrectingDataAccessObjectSaveTest extends HiveTest{
  private final static Log log = LogFactory.getLog(ErrorCorrectingDataAccessObjectSaveTest.class);
  private HiveSessionFactoryImpl hiveSessionFactory;

  @Test
  public void shouldSaveEntities() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport original = WeatherReportImpl.generate();
    dao.save(original);
    WeatherReport fetched = dao.get(original.getReportId());
    assertEquals(original, fetched);
  }

  @Test
  public void shouldSaveManyEntities() throws Exception {
    ErrorCorrectingDataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    Collection<WeatherReport> reports = Lists.newArrayList();
    for(int i=0; i<100; i++)
      reports.add(WeatherReportImpl.generate());
    dao.saveAll(reports);
    for(WeatherReport report : reports)
      assertTrue(dao.exists(report.getReportId()));
  }

  @Test
  public void shouldSaveAndReIndexIfAnEntityExistsOnTheDataNodeButNotTheDirectory() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport report = getPersistentInstance(dao);
    hive.directory().deleteResourceId("WeatherReport", report.getReportId());
    assertFalse(hive.directory().getNodeIdsOfResourceId("WeatherReport", report.getReportId()).size() > 0);
    report.setRegionCode(87654);
    dao.save(report);
    assertTrue(hive.directory().getNodeIdsOfResourceId("WeatherReport", report.getReportId()).size() > 0);
    WeatherReport thawed = dao.get(report.getReportId());
    assertEquals(report, thawed);
  }

  @Test
  public void shouldSaveEntitiesWhenThePartitionKeyChanges() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport report = getPersistentInstance(dao);
    String oldContinent = report.getContinent();
    String newContinent = report.getContinent().equals("Asia") ? "Australia" : "Asia";
    report.setContinent(newContinent);
    dao.save(report);
    Session oldNodeSession = null;
    Session newNodeSession = null;

    try {
      oldNodeSession = hiveSessionFactory.openSession(oldContinent);
      newNodeSession = hiveSessionFactory.openSession(newContinent);
      if(!hive.directory().getNodeIdsOfPrimaryIndexKey(oldContinent).equals(hive.directory().getNodeIdsOfPrimaryIndexKey(newContinent)))
        assertNull(oldNodeSession.get(WeatherReport.class, report.getReportId()));
      WeatherReport fetched = (WeatherReport) newNodeSession.get(WeatherReport.class, report.getReportId());
      assertNotNull(fetched);
      assertEquals(newContinent, fetched.getContinent());
    } finally {
      if(oldNodeSession != null)
        oldNodeSession.close();
      if(newNodeSession != null)
        newNodeSession.close();
    }
  }

  @Test
  public void shouldDeleteRecords() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport report = getPersistentInstance(dao);
    assertTrue(dao.exists(report.getReportId()));
    dao.delete(report.getReportId());
    assertFalse(dao.exists(report.getReportId()));
  }

  @Test
  public void shouldNotThrowAnExceptionWhenDeletingNonExistentRecords() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(WeatherReport.class, getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
    WeatherReport report = WeatherReportImpl.generate();
    assertFalse(dao.exists(report.getReportId()));
    dao.delete(report.getReportId());
    assertFalse(dao.exists(report.getReportId()));
  }

  private HiveSessionFactory getSessionFactory() {
    if(hiveSessionFactory==null)
      hiveSessionFactory = (HiveSessionFactoryImpl) new SingletonHiveSessionFactoryBuilder(getHive(), (List<Class<?>>) getMappedClasses(),new SequentialShardAccessStrategy()).getSessionFactory();
    return hiveSessionFactory;
  }

  private WeatherReport getPersistentInstance(DataAccessObject<WeatherReport, Integer> dao) {
      return dao.save(WeatherReportImpl.generate());
  }

}

