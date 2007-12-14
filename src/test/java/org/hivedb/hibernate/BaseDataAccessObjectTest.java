package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
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
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport original = getPersistentInstance(dao);
		WeatherReport report = dao.get(original.getReportId());
		assertEquals(original, report);
	}
	
	@Test
	public void testFindByProperty() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport report = getPersistentInstance(dao);
		report.setTemperature(101);
		dao.save(report);
		WeatherReport found = Atom.getFirstOrThrow(dao.findByProperty("temperature", 101));
		assertEquals(report, found);
	}
	
	@Test
	public void testDelete() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport original = getPersistentInstance(dao);
		dao.delete(original.getReportId());
		assertNull(dao.get(original.getReportId()));
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
		
		return new BaseDataAccessObjectFactory<WeatherReport,Integer>(
				this.config,
				getMappedClasses(),
				clazz, getHive()).create();
	}
	
	private<T> T getInstance(Class<T> clazz) throws Exception {
		return new GenerateInstance<T>(clazz).generate();
	}
	
	private WeatherReport getPersistentInstance(DataAccessObject<WeatherReport, Integer> dao) throws Exception {
		return dao.save(getInstance(WeatherReport.class));
	}
	
	@Test
	public void testUpdate() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport original = getPersistentInstance(dao);
		WeatherReport updated = dao.get(original.getReportId());
		updated.setLatitude(new BigDecimal(30));
		updated.setLongitude(new BigDecimal(30));
		dao.save(updated);
		WeatherReport persisted = dao.get(updated.getReportId());
		assertEquals(updated, persisted);
		assertFalse(updated.equals(original));
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
	public void testFindByPropertyRange() throws Exception {
		List<WeatherReport> reports = new ArrayList<WeatherReport>();
		for(int i=0; i<5; i++) {
			WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
			report.setReportId(i);
			reports.add(report);
		}
		DataAccessObject<WeatherReport,Integer> dao = getDao(getGeneratedClass());
		dao.saveAll(reports);
		
		Collections.sort(reports, new Comparator<WeatherReport>(){
			public int compare(WeatherReport o1, WeatherReport o2) {
				return o1.getTemperature() - o2.getTemperature();
			}});
		
		Collection<WeatherReport> range = 
			dao.findByPropertyRange(
					"temperature", 
					Atom.getFirst(reports).getTemperature(), Atom.getLast(reports).getTemperature());
		assertEquals(reports.size(), range.size());
		
		Collection<WeatherReport> smallerRange =
			dao.findByPropertyRange("temperature", reports.get(1).getTemperature(), reports.get(reports.size()-2).getTemperature());	
		assertEquals(reports.size()-2, smallerRange.size());
		assertFalse(smallerRange.contains(Atom.getFirst(reports)));
		assertFalse(smallerRange.contains(Atom.getLast(reports)));
	}
	
	@Test
	public void testExists() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		assertFalse(dao.exists(88));
		WeatherReport original = getPersistentInstance(dao);
		assertTrue(dao.exists(original.getReportId()));
	}

}
