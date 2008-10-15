package org.hivedb.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SynchronizedWrapper<T> {
  private final static Log log = LogFactory.getLog(SynchronizedWrapper.class);
  private T item;

  public T get() {return item;}
  public synchronized void replace(T newItem) {
    this.item = newItem;
  }
}

