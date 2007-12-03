package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.ContinentalSchema;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseDataAccessObjectTest extends H2HiveTestCase {
	private EntityHiveConfig config;

	@BeforeMethod
	public void setup() throws Exception {
		this.cleanupAfterEachTest = true;
		this.config = getEntityHiveConfig();
	}
	
	@Test
	public void testGet() throws Exception {
		Integer id = insert();
		WeatherReport report = getDao(getGeneratedClass()).get(id);
		assertNotNull(report);
	}
	
	@Test
	public void testFindByProperty() throws Exception {
		Integer id = insert();
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport report = dao.get(id);
		report.setTemperature(101);
		dao.save(report);
		WeatherReport found = Atom.getFirstOrThrow(dao.findByProperty("temperature", 101));
		assertEquals(report, found);
	}
	
	@Test
	public void testDelete() throws Exception {
		Integer id = insert();
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		dao.delete(id);
		assertNull(dao.get(id));
	}
	
	public Integer insert() throws Exception {
		
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
		dao.save(report);
		WeatherReport savedReport = dao.get(report.getReportId());
		return report.getReportId();
	}
	
	@Test 
	public void testInsert() {			
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
		dao.save(report);
		WeatherReport savedReport = dao.get(report.getReportId());
		assertNotNull(savedReport);
		assertEquals(report, savedReport);
	}

	private Class getGeneratedClass() {
		return GeneratedInstanceInterceptor.newInstance(WeatherReport.class).getClass();
	}

	private DataAccessObject<WeatherReport, Integer> getDao(Class clazz) {
		
		return new DataAccessObjectFactory<WeatherReport,Integer>(
				this.config,
				clazz).create();
	}
	
	@Test
	public void testUpdate() throws Exception {
		Integer id = insert();
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport updated = dao.get(id);
		updated.setLatitude(new BigDecimal(30));
		updated.setLongitude(new BigDecimal(30));
		dao.save(updated);
		WeatherReport persisted = dao.get(id);
		assertEquals(updated, persisted);
	}

	@Test
	public void testSaveAll() throws Exception {
		Collection<WeatherReport> reports = new ArrayList<WeatherReport>();
		for(int i=0; i<5; i++) {
			WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
			report.setReportId(i);
			reports.add(report);
		}
		DataAccessObject<WeatherReport,Integer> dao = getDao(getGeneratedClass());
		dao.saveAll(reports);
		
		for(WeatherReport report : reports)
			assertEquals(report, dao.get(report.getReportId()));
	}
	
	@Test
	public void testUpdateAll() throws Exception {
		Collection<WeatherReport> reports = new ArrayList<WeatherReport>();
		for(int i=0; i<5; i++) {
			WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
			report.setReportId(i);
			reports.add(report);
		}
		DataAccessObject<WeatherReport,Integer> dao = getDao(getGeneratedClass());
		dao.saveAll(reports);


		Collection<WeatherReport> updated = new ArrayList<WeatherReport>();
		for(WeatherReport report : reports){
			report.setTemperature(100);
			updated.add(report);
		}
		dao.saveAll(updated);	
		
		for(WeatherReport report : updated) {
			final WeatherReport weatherReport = dao.get(report.getReportId());
			assertEquals(report, weatherReport);
		}
	}
	
	@Test
	public void testExists() throws Exception {
		assertFalse(getDao(getGeneratedClass()).exists(88));
		Integer id = insert();
		assertTrue(getDao(getGeneratedClass()).exists(id));
	}

}
