package org.hivedb;

import java.util.*;

public class RandomAssigner implements Assigner {
  private Random random = new Random(new Date().getTime());

  public Node chooseNode(Collection<Node> nodes, Object value) {
    if (nodes.size() == 0)
      throw new HiveRuntimeException("The Hive has no Nodes; the Assigner cannot make a choice.");
    else
      return new ArrayList<Node>(nodes).get(random.nextInt(nodes.size()));
  }

  public Collection<Node> chooseNodes(Collection<Node> nodes, Object value) {
    return Arrays.asList(new Node[]{chooseNode(nodes, value)});
  }
}
