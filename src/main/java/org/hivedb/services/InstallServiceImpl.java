package org.hivedb.services;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class InstallServiceImpl implements InstallService {
	private Map<String, Schema> schemata = Maps.newHashMap();
	private Hive hive;
	
	public InstallServiceImpl(Collection<Schema> schemata, Hive hive) {
		this.hive = hive;
		for(Schema s : schemata)
			this.schemata.put(s.getName(), s);
	}
	
	public void addSchema(Schema schema) {schemata.put(schema.getName(), schema);}
	
	public Collection<Schema> getSchemata() {return schemata.values();}
	
	public Boolean install(String schemaName, String nodeName, String dbName,String host, String dialect) {
		return install(schemaName, getOrAddNode(nodeName, dbName, host, dialect).getName());
	}

	private Node getOrAddNode(String nodeName, String dbName, String host,
			String dialect) {
		Node node;
		try {
			node = hive.getNode(nodeName);
		} catch( NoSuchElementException e) {
			node = new Node(nodeName, dbName, host, DialectTools.stringToDialect(dialect));
			try {
				hive.addNode(node);
			} catch (HiveReadOnlyException e1) {
				throw new HiveRuntimeException("Hive was locked read-only.", e1);
			}
		}
		return node;
	}

	public Collection<String> listDialects() {
		return Transform.map(new Unary<HiveDbDialect, String>(){
			public String f(HiveDbDialect item) {
				return DialectTools.dialectToString(item);
			}}, Arrays.asList(HiveDbDialect.values()));
	}

	public Collection<String> listSchemas() {
		return schemata.keySet();
	}

	public Boolean install(String schemaName, String nodeName) {
		Schema s = schemata.get(schemaName);
		String uri = hive.getNode(nodeName).getUri();
		s.install(uri);
		return true;
	}

	public Boolean installAll(String nodeName) {
		Boolean installed = true;
		for(String s : schemata.keySet())
			installed &= install(s, nodeName);
		return installed;
	}

	public Boolean installAll(String nodeName, String dbName, String host,
			String dialect) {
		return installAll(getOrAddNode(nodeName, dbName, host, dialect).getName());
	}

	public Boolean addNode(String nodeName, String dbName, String host,String dialect) {
		try {
			hive.addNode(new Node(nodeName, dbName, host, DialectTools.stringToDialect(dialect)));
			return true;
		} catch (HiveReadOnlyException e) {
			throw new HiveRuntimeException("hive was locked read-only", e);
		}
	}

	public Collection<String> listNodes() {
		return Transform.map(new Unary<Node, String>(){
			public String f(Node item) {
				return item.getName();
			}}, hive.getNodes());
	}

}
