package org.hivedb.management;

import java.io.IOException;
import java.util.Collection;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import mx4j.tools.adaptor.http.HttpAdaptor;
import mx4j.tools.adaptor.http.XSLTProcessor;

import org.apache.log4j.Logger;
import org.springframework.jmx.export.MBeanExporterListener;

public class HttpAdaptorManager implements MBeanExporterListener {
	Logger log = Logger.getLogger(HttpAdaptorManager.class);
	private MBeanServer mbeanServer;
	private Collection<HttpAdaptor> adaptors = null;

	public void mbeanRegistered(ObjectName objectName) {
		// TODO: I don't think we need to be an MBeanExporterListener any longer, as the Spring config 
		// is explicitly invoking start() on the HttpAdaptors via this.startAdaptors()  -jm
	}

	public void mbeanUnregistered(ObjectName objectName) {
		// TODO: I don't think we need to be an MBeanExporterListener any longer, as the Spring config 
		// is explicitly invoking start() on the HttpAdaptors via this.startAdaptors()  -jm
	}
	
	public void startAdaptors() {
		if (this.mbeanServer==null) {
			log.error("The HttpAdaptorManager init method (startAdaptors) was called before a reference to the MBeanServer was set");
		}
		for (HttpAdaptor adaptor : adaptors)
			try {
				adaptor.setProcessor(new XSLTProcessor());
				adaptor.preRegister(this.mbeanServer, new ObjectName("Foo:bar=beef"));
				adaptor.start();
			} catch (IOException e) {
				log.error("Can't start HttpAdaptor: " + e);
			} catch (Exception e) {
				log.error("Can't start HttpAdaptor: " + e);
			}
	}
	
	public void setHttpAdaptors(Collection<HttpAdaptor> adaptors) {
		this.adaptors = adaptors;
	}

	public void setMbeanServer(MBeanServer mbeanServer) {
		this.mbeanServer = mbeanServer;
	}
}
