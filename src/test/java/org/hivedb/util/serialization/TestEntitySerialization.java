package org.hivedb.util.serialization;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEntitySerialization extends H2HiveTestCase {
	
	@Test
	public void testObjectSerialization() {
		for (EntityConfig entityConfig : getEntityHiveConfig().getEntityConfigs()) {
			XmlXStreamSerializer<Object> serializer = createSerializer(entityConfig);
			testObjectSerializationWithNullComplexSubObjects(serializer, entityConfig);
			testObjectSerializationWithComplexSubObjects(serializer, entityConfig);
		}
	}

	private void testObjectSerializationWithNullComplexSubObjects(XmlXStreamSerializer<Object> serializer, EntityConfig entityConfig) {
	
		final Class representedInterface = entityConfig.getRepresentedInterface();
		Object instance = getWrappedInstance(entityConfig);
		for (String propertyName : ReflectionTools.getPropertiesOfCollectionGetters(representedInterface))
			ReflectionTools.invokeSetter(instance, propertyName, null);
		InputStream xmlInputStream = serializer.serialize(instance);
		Object deserialized = serializer.deserialize(xmlInputStream);
		for (String propertyName : ReflectionTools.getPropertiesOfCollectionGetters(representedInterface))
			Assert.assertNull(ReflectionTools.invokeGetter(deserialized, propertyName));
		Assert.assertEquals(instance, deserialized);
	}
	
	private void testObjectSerializationWithComplexSubObjects(XmlXStreamSerializer<Object> serializer, EntityConfig storageConfig) {	
		final Class representedInterface = storageConfig.getRepresentedInterface();
		Object instance = getWrappedInstance(storageConfig);
		InputStream xmlInputStream = serializer.serialize(instance);
		Object deserialized = serializer.deserialize(xmlInputStream);
		for (String propertyName : ReflectionTools.getPropertiesOfCollectionGetters(representedInterface))
			Assert.assertNotNull(ReflectionTools.invokeGetter(deserialized, propertyName));
		Assert.assertEquals(instance, deserialized);
	}

	// 	This test requires that gunzip be in the environment path, which is almost guaranteed
	@Test
	public void testObjectBlobGzipCompatibility()
	{
		for (EntityConfig entityConfig : getEntityHiveConfig().getEntityConfigs()) {
			XmlXStreamSerializer<Object> instanceSerializer = createSerializer(entityConfig);
			Object instance = getWrappedInstance(entityConfig);
			gzipTest(instanceSerializer, instance);
		}
	}

	public static<T> void gzipTest(XmlXStreamSerializer<T> serializer, T instance) {
		// Serialize the instance to a file
		InputStream xmlInputStream = serializer.serialize(instance);
		File tempFile = null;
		try {
			tempFile = File.createTempFile("tmp", ".gz");
			FileOutputStream fileOutputStream = new FileOutputStream(tempFile);	
			byte[] bytes = new byte[1024];
	    
	    	// Write the compressed bytes to the filesystem
			while ((xmlInputStream.read(bytes)) != -1) {
				fileOutputStream.write(bytes);
			}
			fileOutputStream.close();
	    } catch (IOException e) {
			new RuntimeException(e);
		}	
	   
		// Execute gunzip to decompress the file
		List<String> command = new ArrayList<String>();
        command.add("gunzip");
        command.add(tempFile.getAbsolutePath());
        ProcessBuilder builder = new ProcessBuilder(command);
	     try {
	        final Process process = builder.start();
	     
	        InputStream is = process.getErrorStream();
	        InputStreamReader isr = new InputStreamReader(is);
	        BufferedReader errorReader = new BufferedReader(isr);
	        String line = errorReader.readLine();
	        Assert.assertTrue(line == null, String.format("Error stream is not empty: %s", line));
	     }  catch (IOException e) {
				new RuntimeException(e);
		 }
	     
	     try {
	        // Load the unzipped file
	        FileInputStream uncompressedFileInputStream = new FileInputStream(tempFile.getAbsolutePath());
	        // Decompress
	        T uncompressedInstance = serializer.deserialize(uncompressedFileInputStream);
	        Assert.assertEquals(instance, uncompressedInstance);	
	        tempFile.delete();
		} catch (IOException e) {
			new RuntimeException(e);
		}
	}
	
	private XmlXStreamSerializer<Object> createSerializer(EntityConfig entityConfig) {
		return new XmlXStreamSerializer<Object>(entityConfig.getRepresentedInterface());
	}
	
	private Object getWrappedInstance(EntityConfig entityConfig) {
		final Class representedInterface = entityConfig.getRepresentedInterface();
		return new ClassXmlTransformerImpl<Object>(representedInterface)
								.wrapInSerializingImplementation(new GenerateInstance<Object>(representedInterface).generate());
	}
}


