package org.hivedb;

import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

import java.util.Arrays;

public interface Lockable {

  public enum Status {
    writable(0),
    readOnly(1),
    unavailable(2);
    private final int value;   // for database

    Status(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static Status getByValue(final int value) {
      return Filter.grepSingle(new Predicate<Status>() {
        public boolean f(Status status) {
          return status.getValue() == value;
        }
      }, Arrays.asList(Status.values()));
    }
  }

  public Status getStatus();
}