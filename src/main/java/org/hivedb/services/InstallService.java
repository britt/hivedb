package org.hivedb.services;

import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface InstallService {
	@WebMethod
	public Collection<String> listSchemas();
	@WebMethod
	public Collection<String> listDialects();
	@WebMethod
	public Collection<String> listNodes();
	@WebMethod
	public Boolean addNode(String nodeName, String dbName, String host, String dialect, String user, String password);
	@WebMethod
	public Boolean install(String schemaName, String nodeName);
	@WebMethod(operationName="installOnNewNode")
	public Boolean install(String schemaName, String nodeName, String dbName, String host, String dialect, String user, String password);
	@WebMethod
	public Boolean installAll(String nodeName);
	@WebMethod(operationName="installAllOnNewNode")
	public Boolean installAll(String nodeName, String dbName, String host, String dialect, String user, String password);
}
