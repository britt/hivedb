package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.hivedb.util.database.test.HiveTest;

public class DynamicMapDataAccessObjectTest extends HiveTest {
//	private HiveConfig config;
//
//	@BeforeMethod
//	public void setup() throws Exception {
//		ConfigurationReader reader = new ConfigurationReader(WeatherReport.class, Continent.class);
//		this.config = reader.getHiveConfiguration(getHive());
//		getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2));
//		new ContinentalSchema(getConnectString(getHiveDatabaseName())).install();
//		new WeatherSchema(getConnectString(getHiveDatabaseName())).install();
//	}
//	
//	@Test
//	public void testGet() throws Exception {
//		Integer id = testInsert();
//		WeatherReport report = getDao().get(id);
//		assertNotNull(report);
//	}
//	
//	@Test
//	public void testFindByProperty() throws Exception {
//		Integer id = testInsert();
//		DataAccessObject<WeatherReport, Integer> dao = getDao();
//		WeatherReport report = dao.get(id);
//		report.setTemperature(101);
//		dao.save(report);
//		WeatherReport found = Atom.getFirstOrThrow(dao.findByProperty("temperature", 101));
//		assertEquals(report, found);
//	}
//	
//	@Test
//	public void testDelete() throws Exception {
//		Integer id = testInsert();
//		DataAccessObject<WeatherReport, Integer> dao = getDao();
//		dao.delete(id);
//		assertNull(dao.get(id));
//	}
//	
//	@Test
//	public Integer testInsert() throws Exception {
//		DataAccessObject<WeatherReport, Integer> dao = getDao();
//		WeatherReport report = (WeatherReport) WeatherReportImpl.generate();
//		dao.save(report);
//		WeatherReport savedReport = dao.get(report.getReportId());
//		assertEquals(report, savedReport);
//		return report.getReportId();
//	}
//
//	private DataAccessObject<WeatherReport, Integer> getDao() {
//		
//		return new DataAccessObjectFactory<WeatherReport,Integer>(
//				this.config,
//				WeatherReport.class).createDynamicDao();
//	}
//	
//	@Test
//	public void testUpdate() throws Exception {
//		Integer id = testInsert();
//		DataAccessObject<WeatherReport, Integer> dao = getDao();
//		WeatherReport updated = dao.get(id);
//		updated.setLatitude(new Double(30));
//		updated.setLongitude(new Double(30));
//		dao.save(updated);
//		WeatherReport persisted = dao.get(id);
//		assertEquals(updated, persisted);
//	}
//	
//	@Test
//	public void testSaveAll() throws Exception {
//		Collection<WeatherReport> reports = new ArrayList<WeatherReport>();
//		for(int i=0; i<5; i++) {
//			WeatherReport report = (WeatherReport) WeatherReportImpl.generate();
//			report.setReportId(i);
//			reports.add(report);
//		}
//		DataAccessObject<WeatherReport,Integer> dao = getDao();
//		dao.saveAll(reports);
//		
//		for(WeatherReport report : reports)
//			assertEquals(report, dao.get(report.getReportId()));
//	}
//	
//	@Test
//	public void testUpdateAll() throws Exception {
//		Collection<WeatherReport> reports = new ArrayList<WeatherReport>();
//		for(int i=0; i<5; i++) {
//			WeatherReport report = (WeatherReport) WeatherReportImpl.generate();
//			report.setReportId(i);
//			reports.add(report);
//		}
//		DataAccessObject<WeatherReport,Integer> dao = getDao();
//		dao.saveAll(reports);
//
//
//		Collection<WeatherReport> updated = new ArrayList<WeatherReport>();
//		for(WeatherReport report : reports){
//			report.setTemperature(100);
//			updated.add(report);
//		}
//		dao.saveAll(updated);	
//		
//		for(WeatherReport report : updated)
//			assertEquals(report, dao.get(report.getReportId()));
//	}
//	
//	@Test
//	public void testExists() throws Exception {
//		assertFalse(getDao().exists(88));
//		Integer id = testInsert();
//		assertTrue(getDao().exists(id));
//	}

}
