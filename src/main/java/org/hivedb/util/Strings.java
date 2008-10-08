package org.hivedb.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Iterator;

public class Strings {
  private final static Log log = LogFactory.getLog(Strings.class);

  public static String join(String separator, String... strings) {
    StringBuilder sb = new StringBuilder();
    List<String> list = Lists.newList(strings);
    Iterator<String> itr = list.iterator();
    while (itr.hasNext()) {
      sb.append(itr.next());
      if (itr.hasNext())
        sb.append(separator);
    }
    return sb.toString();
  }
}

