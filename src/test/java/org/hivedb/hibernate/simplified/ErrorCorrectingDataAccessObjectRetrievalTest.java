package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Lockable;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.WeatherReport;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.List;

@HiveTest.Config("hive_default")
public class ErrorCorrectingDataAccessObjectRetrievalTest extends HiveTest {
  private final static Log log = LogFactory.getLog(ErrorCorrectingDataAccessObjectRetrievalTest.class);
  private HiveSessionFactory hiveSessionFactory;

  @Test
	public void shouldRetrieveAnExistingEntity() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(getGeneratedClass(), getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport original = getPersistentInstance(dao);
		WeatherReport report = dao.get(original.getReportId());
		assertEquals(ReflectionTools.getDifferingFields(original, report, WeatherReport.class).toString(), original.hashCode(), report.hashCode());
	}

  @Test
  public void shouldDeleteTheDirectoryEntryIfTheEntityIsNotPresentOnTheDataNode() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(getGeneratedClass(), getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
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
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(getGeneratedClass(), getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport report = dao.get(777777);
		assertTrue(report == null);
  }

  @Test
  public void shouldReturnNullIfTheRecordExistsButHasNoDirectoryEntry() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(getGeneratedClass(), getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
		WeatherReport original = getPersistentInstance(dao);
    String resourceName = getEntityHiveConfig().getEntityConfig(WeatherReport.class).getResourceName();
    getHive().directory().deleteResourceId(resourceName, original.getReportId());
    assertFalse(dao.exists(original.getReportId()));    
    WeatherReport report = dao.get(original.getReportId());
    assertTrue(report == null);
  }

  @Test
  public void shouldOnlyWarnIfTheHiveIsLockedForWrites() throws Exception {
    DataAccessObject<WeatherReport, Integer> dao = new ErrorCorrectingDataAccessObject<WeatherReport, Integer>(getGeneratedClass(), getEntityHiveConfig().getEntityConfig(WeatherReport.class),getHive(), getSessionFactory());
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
      hiveSessionFactory = new HiveSessionFactoryBuilderImpl(getHive().getUri(), (List<Class<?>>) getMappedClasses(),new SequentialShardAccessStrategy());
    return hiveSessionFactory;
  }

  private WeatherReport getPersistentInstance(DataAccessObject<WeatherReport, Integer> dao) {
      return dao.save(getInstance(WeatherReport.class));
  }

  private<T> T getInstance(Class<T> clazz) {
    return new GenerateInstance<T>(clazz).generate();
  }

  private Class getGeneratedClass() {
		return GeneratedClassFactory.newInstance(WeatherReport.class).getClass();
	}
}

