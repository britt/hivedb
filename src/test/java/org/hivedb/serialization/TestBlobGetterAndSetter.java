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
		XmlXStreamSerializationProvider.initialize(Collections.singletonList((Class)SimpleBlobject.class));
	}
	
	@Test
	public void testGet() throws Exception {
		SimpleBlobject blobject = getGenerator().generate();
		Blob blob = (Blob) new BlobGetter().get(blobject);
		assertNotNull(blob);
		Object thawed = getSerializer().deserialize(blob.getBinaryStream());
		assertEquals(getGenerator().generateAndCopyProperties(blobject), thawed);
	}

	private Serializer<Object, InputStream> getSerializer() {
		return XmlXStreamSerializationProvider.instance().getSerializer((Class)SimpleBlobject.class);
	}
	
	@Test
	public void testSet() throws Exception {
		SimpleBlobject target = getGenerator().generate();
		SimpleBlobject updated = getGenerator().generate();
		assertFalse(target.equals(updated));
		new BlobSetter().set(target, new BlobGetter().get(updated), null);
		assertEquals(getGenerator().generateAndCopyProperties(updated), target);
	}

	private GenerateInstance<SimpleBlobject> getGenerator() {
		return new GenerateInstance<SimpleBlobject>(SimpleBlobject.class);
	}
}
