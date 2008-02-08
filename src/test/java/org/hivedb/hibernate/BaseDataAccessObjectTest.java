package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.MySqlHiveTestCase;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.functional.Amass;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.Joiner;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseDataAccessObjectTest extends H2HiveTestCase {
	private EntityHiveConfig config;

	private static Random random = new Random();
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
		assertEquals(ReflectionTools.getDifferingFields(original, report, WeatherReport.class).toString(), original.hashCode(), report.hashCode());
	}
	
	@Test
	public void testFindByProperty() throws Exception {
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
		dao.save(report);
		int temperature = random.nextInt();
		GeneratedInstanceInterceptor.setProperty(report, "temperature", temperature);
		dao.save(report);
		WeatherReport found = Atom.getFirstOrThrow(dao.findByProperty("temperature", temperature));
		Assert.assertEquals(report.hashCode(), found.hashCode(), ReflectionTools.getDifferingFields(report, found, WeatherReport.class).toString());
		found = Atom.getFirstOrThrow(dao.findByProperty("regionCode", report.getRegionCode()));
		assertEquals(report.hashCode(), found.hashCode());
		found = Atom.getFirstOrThrow(dao.findByProperty("weatherEvents", Atom.getFirstOrThrow(report.getWeatherEvents()).getEventId()));
		assertEquals(report.hashCode(), found.hashCode());
		found = Atom.getFirstOrThrow(dao.findByProperty("continent", report.getContinent()));
		assertEquals(report.hashCode(), found.hashCode());	
		found = Atom.getFirstOrThrow(dao.findByProperty("sources", Atom.getFirstOrThrow(report.getSources())));
		assertEquals(report.hashCode(), found.hashCode());
	}
	
	@Test
	public void testFindByPropertyPaged() throws Exception {
		final DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		Collection<WeatherReport> reports =
			Generate.create(new Generator<WeatherReport>() {
				public WeatherReport generate() {
					WeatherReport report =  new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
					GeneratedInstanceInterceptor.setProperty(report, "continent", "Derkaderkastan");
					GeneratedInstanceInterceptor.setProperty(report, "temperature", 101);
					GeneratedInstanceInterceptor.setProperty(report, "sources", Arrays.asList(new Integer[] {101, 102,103}));
					return dao.save(report);
				}}, new NumberIterator(12));	
		
		// Test a scalar property
		// TODO get this working with a collection primitive "sources"
		for (final String property : new String[] {"temperature"}) {
			Assert.assertEquals(dao.findByProperty(property, 101).size(), 12);
			final Collection<WeatherReport> results = Filter.grepUnique(Transform.flatten(Transform.map(new Unary<Integer, Collection<WeatherReport>>() {
				public Collection<WeatherReport> f(Integer i) {
					final Collection<WeatherReport> findByProperty = dao.findByProperty(property, 101 ,(i-1)*4, 4);
					return findByProperty;
				}
			}, new NumberIterator(3))));
			final HashSet<WeatherReport> retrievedSet = new HashSet<WeatherReport>(results);
			Assert.assertEquals(
					retrievedSet.size(),
					reports.size());
			Assert.assertEquals(
					retrievedSet.hashCode(),
					new HashSet(reports).hashCode());
		}
		
		
	}
	

	@Test
	public void testFindByPropertyRange() throws Exception {
		
		Collection<WeatherReport> reports =
			Generate.create(new Generator<WeatherReport>() {
				public WeatherReport generate() {
					return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
				}}, new NumberIterator(5));
		
		DataAccessObject<WeatherReport,Integer> dao = getDao(getGeneratedClass());
		dao.saveAll(reports);
		Collection<WeatherReport> x = dao.findByProperty("temperature", Atom.getFirst(reports).getTemperature());
		Integer min = Amass.min(
				new Unary<WeatherReport, Integer>() {
					public Integer f(WeatherReport weatherReport) {
						return weatherReport.getTemperature();
					}},
				reports,
				Integer.class);
		Integer max = Amass.max(
				new Unary<WeatherReport, Integer>() {
					public Integer f(WeatherReport weatherReport) {
						return weatherReport.getTemperature();
					}},
				reports,
				Integer.class);
		Collection<WeatherReport> range = dao.findByPropertyRange("temperature",  min, max);
		assertEquals(reports.size(), range.size());
		
		Collection<WeatherReport> smallerRange =
			dao.findByPropertyRange("temperature", Atom.getFirst(reports).getTemperature(), Atom.getFirst(reports).getTemperature());	
		assertEquals(1, smallerRange.size());
	}
	
	@Test
	public void testFindByPropertyRangePaged() throws Exception {
		final DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		final int INSTANCE_COUNT = 12;
		Collection<WeatherReport> set = new HashSet<WeatherReport>();
		int min=0, max=0;
		for (int i=0; i<INSTANCE_COUNT; i++) {
			int temperature = random.nextInt();
			min = Math.min(min, temperature);
			max = Math.max(max, temperature);
			WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
			GeneratedInstanceInterceptor.setProperty(report, "temperature", temperature);
			dao.save(report);
			set.add(report);
		}
		final int finalMin = min;
		final int finalMax = max;
		Assert.assertEquals(
				new HashSet<WeatherReport>(Transform.flatten(Transform.map(new Unary<Integer, Collection<WeatherReport>>() {
					public Collection<WeatherReport> f(Integer i) {
						final Collection<WeatherReport> value = dao.findByPropertyRange("temperature", finalMin, finalMax, (i-1)*4, 4);
						return value;
					}
				}, new NumberIterator(3)))).hashCode(),
				set.hashCode());
	}
		
	@Test
	public void testGetCount() throws Exception {
		
		final int temperature = random.nextInt();
		final List<String> partitionDimensionKeys = Arrays.asList(new String[] {"Asia", "Andromida"});
		Collection<WeatherReport> reports =
			Generate.create(new Generator<WeatherReport>() {
				public WeatherReport generate() {
					WeatherReport weatherReport =  new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
					// Set the same temperature for each partition dimension id. The partition dimension id will be calculated from the report id
					GeneratedInstanceInterceptor.setProperty(weatherReport, "temperature", temperature + weatherReport.getReportId() % 2);
					GeneratedInstanceInterceptor.setProperty(weatherReport, "continent", partitionDimensionKeys.get(weatherReport.getReportId() % 2));
					return weatherReport;
				}}, new NumberIterator(5));
		
		DataAccessObject<WeatherReport,Integer> dao = getDao(getGeneratedClass());
		dao.saveAll(reports);
		Assert.assertEquals(dao.getCount("temperature", temperature) + dao.getCount("temperature", temperature+1), 5);
		Assert.assertEquals(dao.getCountByRange("temperature", temperature, temperature+1), (Integer)5);
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
		assertEquals(report.hashCode(), savedReport.hashCode());
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
		GeneratedInstanceInterceptor.setProperty(updated, "latitude", new Double(30));
		GeneratedInstanceInterceptor.setProperty(updated, "longitude", new Double(30));
		
		/* TODO this fails because we currently don't support one-to-many relationships. We need to reenable the
		  deleteOrphanItems in our BaseDataAccessObject to support this
		 */
	/*
		// Test collection item updates
		List<WeatherEvent> weatherEvents = new ArrayList<WeatherEvent>(original.getWeatherEvents());
		// Delete the first
		weatherEvents.remove(0);
		// Update the second
		weatherEvents.get(0).setName("foobar");
		// Add a third
		weatherEvents.add(new GenerateInstance<WeatherEvent>(WeatherEvent.class).generate());
		GeneratedInstanceInterceptor.setProperty(updated, "weatherEvents", weatherEvents);
		dao.save(updated);
		final WeatherReport persisted = dao.get(updated.getReportId());
		assertFalse(updated.equals(original));
		// Check the updated collection
			// size should be equal
		assertEquals(original.getWeatherEvents().size(), persisted.getWeatherEvents().size());
			// first item should be removed
		assertFalse(Filter.grepItemAgainstList(Atom.getFirst(original.getWeatherEvents()), persisted.getWeatherEvents()));
			// should be an updated item named foobar
		assertTrue(Filter.grepItemAgainstList("foobar", 
				Transform.map(new Unary<WeatherEvent,String>() {
					public String f(WeatherEvent weatherEvent) {
						return weatherEvent.getName();
				}}, persisted.getWeatherEvents())));
			// new item should exist
		assertTrue(Filter.grepItemAgainstList(Atom.getLast(weatherEvents), persisted.getWeatherEvents()));
		*/
	}

	@Test
	public void testSaveAll() throws Exception {
		Collection<WeatherReport> reports = new ArrayList<WeatherReport>();
		for(int i=0; i<5; i++) {
			WeatherReport report = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
			GeneratedInstanceInterceptor.setProperty(report, "reportId", i);
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
			GeneratedInstanceInterceptor.setProperty(report, "reportId", i);
			reports.add(report);
		}
		DataAccessObject<WeatherReport,Integer> dao = getDao(getGeneratedClass());
		dao.saveAll(reports);


		Collection<WeatherReport> updated = new ArrayList<WeatherReport>();
		for(WeatherReport report : reports){
			GeneratedInstanceInterceptor.setProperty(report, "temperature", 100);
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
		DataAccessObject<WeatherReport, Integer> dao = getDao(getGeneratedClass());
		assertFalse(dao.exists(88));
		WeatherReport original = getPersistentInstance(dao);
		assertTrue(dao.exists(original.getReportId()));
	}

}
