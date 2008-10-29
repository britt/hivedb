package org.hivedb.services;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Node;
import org.hivedb.persistence.Schema;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

import javax.jws.WebService;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

@WebService(endpointInterface = "org.hivedb.services.InstallService")
public class InstallServiceImpl implements InstallService {
  private Map<String, Schema> schemata = Maps.newHashMap();
  private Hive hive;

  public InstallServiceImpl(Collection<Schema> schemata, Hive hive) {
    this.hive = hive;
    for (Schema s : schemata)
      this.schemata.put(s.getName(), s);
  }

  public void addSchema(Schema schema) {
    schemata.put(schema.getName(), schema);
  }

  public Collection<Schema> getSchemata() {
    return schemata.values();
  }

  public Boolean install(String schemaName, String nodeName, String dbName, String host, String dialect, String user, String password) {
    return install(schemaName, getOrAddNode(nodeName, dbName, host, dialect, user, password).getName());
  }

  private Node getOrAddNode(String nodeName, String dbName, String host, String dialect, String user, String password) {
    Node node;
    try {
      node = hive.getNode(nodeName);
    } catch (NoSuchElementException e) {
      node = new Node(nodeName, dbName, host, DialectTools.stringToDialect(dialect));
      node.setUsername(user);
      node.setPassword(password);
      try {
        hive.addNode(node);
      } catch (HiveLockableException e1) {
        throw new HiveRuntimeException("Hive was locked read-only.", e1);
      }
    }
    return node;
  }

  public Collection<String> listDialects() {
    return Transform.map(new Unary<HiveDbDialect, String>() {
      public String f(HiveDbDialect item) {
        return DialectTools.dialectToString(item);
      }
    }, Arrays.asList(HiveDbDialect.values()));
  }

  public Collection<String> listSchemas() {
    return schemata.keySet();
  }

  public Boolean install(String schemaName, String nodeName) {
    Schema s = schemata.get(schemaName);
    String uri = hive.getNode(nodeName).getUri();
    Schemas.install(s, uri);
    return true;
  }

  public Boolean installAll(String nodeName) {
    Boolean installed = true;
    for (String s : schemata.keySet())
      installed &= install(s, nodeName);
    return installed;
  }

  public Boolean installAll(String nodeName, String dbName, String host,
                            String dialect, String user, String password) {
    return installAll(getOrAddNode(nodeName, dbName, host, dialect, user, password).getName());
  }

  public Boolean addNode(String nodeName, String dbName, String host, String dialect, String user, String password) {
    try {
      Node node = new Node(nodeName, dbName, host, DialectTools.stringToDialect(dialect));
      node.setUsername(user);
      node.setPassword(password);
      hive.addNode(node);
      return true;
    } catch (HiveLockableException e) {
      throw new HiveRuntimeException("hive was locked read-only", e);
    }
  }

  public Collection<String> listNodes() {
    return Transform.map(new Unary<Node, String>() {
      public String f(Node item) {
        return item.getName();
      }
    }, hive.getNodes());
  }
}
