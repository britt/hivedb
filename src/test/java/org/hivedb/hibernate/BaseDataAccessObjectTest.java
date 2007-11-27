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
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseDataAccessObjectTest extends H2HiveTestCase {
	private EntityHiveConfig config;

	@BeforeMethod
	public void setup() throws Exception {
		this.config = getEntityHiveConfig();
		getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2));
		new ContinentalSchema(getConnectString(getHiveDatabaseName())).install();
		new WeatherSchema(getConnectString(getHiveDatabaseName())).install();
	}
	
	@Test
	public void testGet() throws Exception {
		Integer id = testInsert();
		WeatherReport report = getDao().get(id);
		assertNotNull(report);
	}
	
	@Test
	public void testFindByProperty() throws Exception {
		Integer id = testInsert();
		DataAccessObject<WeatherReportImpl, Integer> dao = getDao();
		WeatherReportImpl report = dao.get(id);
		report.setTemperature(101);
		dao.save(report);
		WeatherReport found = Atom.getFirstOrThrow(dao.findByProperty("temperature", 101));
		assertEquals(report, found);
	}
	
	@Test
	public void testDelete() throws Exception {
		Integer id = testInsert();
		DataAccessObject<WeatherReportImpl, Integer> dao = getDao();
		dao.delete(id);
		assertNull(dao.get(id));
	}
	
	@Test
	public Integer testInsert() throws Exception {
		DataAccessObject<WeatherReportImpl, Integer> dao = getDao();
		WeatherReportImpl report = (WeatherReportImpl) WeatherReportImpl.generate();
		dao.save(report);
		WeatherReport savedReport = dao.get(report.getReportId());
		assertEquals(report, savedReport);
		return report.getReportId();
	}

	private DataAccessObject<WeatherReportImpl, Integer> getDao() {
		
		return new DataAccessObjectFactory<WeatherReportImpl,Integer>(
				this.config,
				WeatherReportImpl.class).create();
	}
	
	@Test
	public void testUpdate() throws Exception {
		Integer id = testInsert();
		DataAccessObject<WeatherReportImpl, Integer> dao = getDao();
		WeatherReportImpl updated = dao.get(id);
		updated.setLatitude(new BigDecimal(30));
		updated.setLongitude(new BigDecimal(30));
		dao.save(updated);
		WeatherReport persisted = dao.get(id);
		assertEquals(updated, persisted);
	}
	
	@Test
	public void testSaveAll() throws Exception {
		Collection<WeatherReportImpl> reports = new ArrayList<WeatherReportImpl>();
		for(int i=0; i<5; i++) {
			WeatherReportImpl report = (WeatherReportImpl) WeatherReportImpl.generate();
			report.setReportId(i);
			reports.add(report);
		}
		DataAccessObject<WeatherReportImpl,Integer> dao = getDao();
		dao.saveAll(reports);
		
		for(WeatherReport report : reports)
			assertEquals(report, dao.get(report.getReportId()));
	}
	
	@Test
	public void testUpdateAll() throws Exception {
		Collection<WeatherReportImpl> reports = new ArrayList<WeatherReportImpl>();
		for(int i=0; i<5; i++) {
			WeatherReportImpl report = (WeatherReportImpl) WeatherReportImpl.generate();
			report.setReportId(i);
			reports.add(report);
		}
		DataAccessObject<WeatherReportImpl,Integer> dao = getDao();
		dao.saveAll(reports);


		Collection<WeatherReportImpl> updated = new ArrayList<WeatherReportImpl>();
		for(WeatherReportImpl report : reports){
			report.setTemperature(100);
			updated.add(report);
		}
		dao.saveAll(updated);	
		
		for(WeatherReport report : updated)
			assertEquals(report, dao.get(report.getReportId()));
	}
	
	@Test
	public void testExists() throws Exception {
		assertFalse(getDao().exists(88));
		Integer id = testInsert();
		assertTrue(getDao().exists(id));
	}

}
