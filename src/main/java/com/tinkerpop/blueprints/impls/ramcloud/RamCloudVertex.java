package com.tinkerpop.blueprints.impls.ramcloud;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class RamCloudVertex extends RamCloudElement implements Vertex {

  public RamCloudVertex(long id, RamCloudGraph graph) {
    // TODO Auto-generated constructor stub
    super(id, graph);
  }

  @Override
  public Edge addEdge(String arg0, Vertex arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<Edge> getEdges(Direction arg0, String... arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VertexQuery query() {
    // TODO Auto-generated method stub
    return null;
  }

}
