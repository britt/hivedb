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

}
