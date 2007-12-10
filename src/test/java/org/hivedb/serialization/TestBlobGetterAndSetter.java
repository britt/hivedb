package org.hivedb.serialization;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.InputStream;
import java.sql.Blob;
import java.util.Collections;

import org.hivedb.util.GenerateInstance;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.serialization.Serializer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestBlobGetterAndSetter {

	@BeforeClass
	public void initializeSerializationProvider() {
		XmlXStreamSerializationProvider.initialize(Collections.singletonList((Class)WeatherReport.class));
	}
	
	@Test
	public void testGet() throws Exception {
		WeatherReport report = WeatherReportImpl.generate();
		Blob blob = (Blob) new BlobGetter().get(report);
		assertNotNull(blob);
		Object thawed = getSerializer().deserialize(blob.getBinaryStream());
		assertEquals(getGenerator().generateAndCopyProperties(report), thawed);
	}

	private Serializer<Object, InputStream> getSerializer() {
		return XmlXStreamSerializationProvider.instance().getSerializer((Class)WeatherReport.class);
	}
	
	@Test
	public void testSet() throws Exception {
		WeatherReport target = getGenerator().generate();
		WeatherReport updated = WeatherReportImpl.generate();
		assertFalse(target.equals(updated));
		new BlobSetter().set(target, new BlobGetter().get(updated), null);
		assertEquals(getGenerator().generateAndCopyProperties(updated), target);
	}

	private GenerateInstance<WeatherReport> getGenerator() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class);
	}
}
