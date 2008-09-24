package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.hibernate.simplified.session.HiveSessionFactory;
import org.hivedb.hibernate.simplified.session.SingletonHiveSessionFactoryBuilder;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.WeatherReport;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.List;

@HiveTest.Config("hive_default")
public class ErrorCorrectingDataAccessObjectSaveTest extends HiveTest{
  private final static Log log = LogFactory.getLog(ErrorCorrectingDataAccessObjectSaveTest.class);
  private HiveSessionFactory hiveSessionFactory;

  @Test
  public void shouldPassATest() throws Exception {
    assertTrue(true);
  }

  private HiveSessionFactory getSessionFactory() {
    if(hiveSessionFactory==null)
      hiveSessionFactory = new SingletonHiveSessionFactoryBuilder(getHive(), (List<Class<?>>) getMappedClasses(),new SequentialShardAccessStrategy()).getSessionFactory();
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

