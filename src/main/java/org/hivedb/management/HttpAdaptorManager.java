package org.hivedb.management;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.springframework.jmx.export.MBeanExporterListener;

public class HttpAdaptorManager implements MBeanExporterListener {
	private MBeanServer mbeanServer;
	private String adaptorSubstring = "httpadaptor";

	public void mbeanRegistered(ObjectName objectName) {
		if (objectName.getCanonicalName().toLowerCase().contains(adaptorSubstring)) {
			try {
				mbeanServer.invoke(objectName, "start", null, null);
			} catch (Exception e) {
				System.err.println("Can't start HttpAdaptor: " + e);
			}
		}
	}

	public void mbeanUnregistered(ObjectName objectName) {
		if (objectName.getCanonicalName().toLowerCase().contains(adaptorSubstring)) {
			try {
				mbeanServer.invoke(objectName, "stop", null, null);
			} catch (Exception e) {
				System.err.println("Can't stop HttpAdaptor: " + e);
			}
		}
	}

	public void setMbeanServer(MBeanServer mbeanServer) {
		this.mbeanServer = mbeanServer;
	}

	public void setAdaptorName(String adaptorName) {
		this.adaptorSubstring = adaptorName;
	}
}
