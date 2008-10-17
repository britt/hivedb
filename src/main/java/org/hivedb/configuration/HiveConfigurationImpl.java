package org.hivedb.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.HiveUtils;

import java.util.Collection;

public class HiveConfigurationImpl implements HiveConfiguration {
  private final static Log log = LogFactory.getLog(HiveConfigurationImpl.class);
  private Collection<Node> nodes;
  private HiveSemaphore semaphore;
  private PartitionDimension partitionDimension;
  private String uri;

  public HiveConfigurationImpl(String uri, Collection<Node> nodes, PartitionDimension partitionDimension, HiveSemaphore semaphore) {
    this.uri = uri;
    this.nodes = nodes;
    this.partitionDimension = partitionDimension;
    this.semaphore = semaphore;
  }

  public Collection<Node> getNodes() {
    return nodes;
  }

  public PartitionDimension getPartitionDimension() {
    return partitionDimension;
  }

  public HiveSemaphore getSemaphore() {
    return semaphore;
  }

  public String getUri() {
    return uri;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HiveConfigurationImpl)) return false;

    HiveConfigurationImpl that = (HiveConfigurationImpl) o;

    if (nodes != null ? !nodes.equals(that.nodes) : that.nodes != null) return false;
    if (partitionDimension != null ? !partitionDimension.equals(that.partitionDimension) : that.partitionDimension != null)
      return false;
    if (semaphore != null ? !semaphore.equals(that.semaphore) : that.semaphore != null) return false;
    if (uri != null ? !uri.equals(that.uri) : that.uri != null) return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = (nodes != null ? nodes.hashCode() : 0);
    result = 31 * result + (semaphore != null ? semaphore.hashCode() : 0);
    result = 31 * result + (partitionDimension != null ? partitionDimension.hashCode() : 0);
    result = 31 * result + (uri != null ? uri.hashCode() : 0);
    return result;
  }

  public HiveDbDialect getDialect() {
    return DriverLoader.discernDialect(getUri());
  }

  @Override
  public String toString() {
    return HiveUtils.toDeepFormatedString(uri, semaphore);
  }
}

