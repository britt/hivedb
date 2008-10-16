package org.hivedb.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.functional.Factory;

import java.util.Observable;
import java.util.Observer;

public class SynchronizedSingletonProvider<T> implements Observer {
  private final static Log log = LogFactory.getLog(SynchronizedSingletonProvider.class);

  private Factory<T> factory;
  private SynchronizedWrapper<T> wrapper = null;

  public SynchronizedSingletonProvider(Factory<T> factory) {
    this.factory = factory;
  }

  public SynchronizedWrapper<T> getSynchronizedInstance() {
    if (wrapper == null)
      wrapper = new SynchronizedWrapper<T>(factory.newInstance());
    return wrapper;
  }

  public void update(Observable o, Object arg) {
    T newInstance = factory.newInstance();
    wrapper.replace(newInstance);
  }
}

