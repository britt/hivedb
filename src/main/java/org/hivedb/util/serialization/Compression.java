package org.hivedb.util.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Compression {
	public static InputStream compress(String xml) {
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(xml.getBytes());
	    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		GZIPOutputStream gZipOutputStream;
		try {
			gZipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	   
	    byte[] buffer = new byte[1024];
	    try {
			while ((byteArrayInputStream.read(buffer)) != -1) {
				gZipOutputStream.write(buffer);
			}
			gZipOutputStream.finish();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	    return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
	}
	
	public static String decompress(InputStream inputStream) {
		
	    GZIPInputStream gZipInputStream;
		try {
			gZipInputStream = new GZIPInputStream(inputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// Use an input stream reader to convert the bytes to chars
	    InputStreamReader inputStreamReader = new InputStreamReader(gZipInputStream);
	    StringBuffer stringBuffer = new StringBuffer();
	    char[] charBuffer = new char[1024];
	    try {
			while ((inputStreamReader.read(charBuffer)) != -1) {
			  stringBuffer.append(charBuffer);
			}
		} catch (IOException e) {
			new RuntimeException(e);
		}
	    return stringBuffer.toString();
	}
}
