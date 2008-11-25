package org.hivedb;

import org.hivedb.util.IdAndNameIdentifiable;
import org.hivedb.util.database.HiveDbDialect;

public interface Node extends IdAndNameIdentifiable<Integer>, Lockable {
  int getPort();

  String getHost();

  String getDatabaseName();

  String getUsername();

  String getPassword();

  String getOptions();

  HiveDbDialect getDialect();

  void setId(int id);

  Integer getId();

  Status getStatus();

  void setStatus(Status status);

  String getUri();

  String getName();

  void setId(Integer id);

  void setName(String name);

  double getCapacity();

  void setUsername(String user);

  void setPassword(String password);
}
