package org.hivedb.util.database.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.Lists;

import java.util.Collection;
import java.util.Random;

public class WeatherEventImpl implements WeatherEvent{
  private final static Log log = LogFactory.getLog(WeatherEventImpl.class);
  private Integer eventId;
  private String name;
  private Collection<Integer> statistics;

  public static WeatherEventImpl generate() {
    WeatherEventImpl event = new WeatherEventImpl();
    event.setEventId(new Random().nextInt());
    event.setName("Event!");
    event.setStatistics(Lists.newList(1,2,3,4));
    return event;
  }

  public Integer getEventId() {
    return eventId;
  }

  public void setEventId(Integer eventId) {
    this.eventId = eventId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Collection<Integer> getStatistics() {
    return statistics;
  }

  public void setStatistics(Collection<Integer> statistics) {
    this.statistics = statistics;
  }
}

