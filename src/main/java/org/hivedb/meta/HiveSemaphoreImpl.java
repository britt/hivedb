/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.Lockable;

/**
 * HiveSemaphore coordinates the Hive-global write lock, and provides a Hive revision counter
 * which indicates signals that a new partition dimension, secondary partition, or node is introduced.
 *
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class HiveSemaphoreImpl implements HiveSemaphore {
  private Lockable.Status status = Lockable.Status.writable;
  private int revision = 0;

  public HiveSemaphoreImpl() {
  }

  public void setRevision(int revision) {
    this.revision = revision;
  }

  public HiveSemaphoreImpl(Lockable.Status status, int revision) {
    this.status = status;
    this.revision = revision;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public int getRevision() {
    return revision;
  }

  public void incrementRevision() {
    revision++;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HiveSemaphore)) return false;

    HiveSemaphore that = (HiveSemaphore) o;

    if (revision != that.getRevision()) return false;
    if (status != that.getStatus()) return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = (status != null ? status.hashCode() : 0);
    result = 31 * result + revision;
    return result;
  }
}
