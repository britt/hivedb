package org.hivedb.hibernate.simplified.session;

import org.hibernate.Criteria;

public interface HiveCriteria extends Criteria {
  public Criteria addRangeRestriction(String propertyName, Object minValue, Object maxValue);
}
