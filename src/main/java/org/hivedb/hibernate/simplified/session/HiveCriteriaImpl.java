package org.hivedb.hibernate.simplified.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.*;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.Restrictions;
import org.hibernate.transform.ResultTransformer;
import org.hivedb.util.classgen.ReflectionTools;

import java.io.Serializable;
import java.util.List;

public class HiveCriteriaImpl implements HiveCriteria, Serializable {
  private final static Log log = LogFactory.getLog(HiveCriteriaImpl.class);
  private Criteria criteria;
  private Class<?> clazz;

  public HiveCriteriaImpl(Criteria criteria, Class<?> clazz) {
    this.criteria = criteria;
    this.clazz = clazz;
  }

  public Criteria addRangeRestriction(String propertyName, Object minValue, Object maxValue) {
    if (ReflectionTools.isComplexCollectionItemProperty(clazz, propertyName)) {
      criteria.createCriteria(propertyName).add(Restrictions.between("id", minValue, maxValue));
    } else {
      criteria.add(Restrictions.between(propertyName, minValue, maxValue));
    }
    return this;
  }

  public String getAlias() {
    return criteria.getAlias();
  }

  public Criteria setProjection(Projection projection) {
    criteria.setProjection(projection);
    return this;
  }

  public Criteria add(Criterion criterion) {
    criteria.add(criterion);
    return this;
  }

  public Criteria addOrder(Order order) {
    criteria.addOrder(order);
    return this;
  }

  public Criteria setFetchMode(String s, FetchMode fetchMode) {
    criteria.setFetchMode(s, fetchMode);
    return this;
  }

  public Criteria setLockMode(LockMode lockMode) {
    criteria.setLockMode(lockMode);
    return this;
  }

  public Criteria setLockMode(String s, LockMode lockMode) {
    criteria.setLockMode(s, lockMode);
    return this;
  }

  public Criteria createAlias(String s, String s1) {
    return criteria.createAlias(s, s1);
  }

  public Criteria createAlias(String s, String s1, int i) {
    return criteria.createAlias(s, s1, i);
  }

  public Criteria createCriteria(String s) {
    return criteria.createCriteria(s);
  }

  public Criteria createCriteria(String s, int i) {
    return criteria.createCriteria(s, i);
  }

  public Criteria createCriteria(String s, String s1) {
    return criteria.createCriteria(s, s1);
  }

  public Criteria createCriteria(String s, String s1, int i) {
    return criteria.createCriteria(s, s1, i);
  }

  public Criteria setResultTransformer(ResultTransformer resultTransformer) {
    criteria.setResultTransformer(resultTransformer);
    return this;
  }

  public Criteria setMaxResults(int i) {
    criteria.setMaxResults(i);
    return this;
  }

  public Criteria setFirstResult(int i) {
    criteria.setFirstResult(i);
    return this;
  }

  public Criteria setFetchSize(int i) {
    criteria.setFetchSize(i);
    return this;
  }

  public Criteria setTimeout(int i) {
    criteria.setTimeout(i);
    return this;
  }

  public Criteria setCacheable(boolean b) {
    criteria.setCacheable(b);
    return this;
  }

  public Criteria setCacheRegion(String s) {
    criteria.setCacheRegion(s);
    return this;
  }

  public Criteria setComment(String s) {
    criteria.setComment(s);
    return this;
  }

  public Criteria setFlushMode(FlushMode flushMode) {
    criteria.setFlushMode(flushMode);
    return this;
  }

  public Criteria setCacheMode(CacheMode cacheMode) {
    criteria.setCacheMode(cacheMode);
    return this;
  }

  public List list() throws HibernateException {
    return criteria.list();
  }

  public ScrollableResults scroll() {
    return criteria.scroll();
  }

  public ScrollableResults scroll(ScrollMode scrollMode) {
    return criteria.scroll(scrollMode);
  }

  public Object uniqueResult() throws HibernateException {
    return criteria.uniqueResult();
  }
}

