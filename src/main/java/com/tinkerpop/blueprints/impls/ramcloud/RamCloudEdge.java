package com.tinkerpop.blueprints.impls.ramcloud;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

public class RamCloudEdge extends RamCloudElement implements Edge {

  public RamCloudVertex outVertex;
  public RamCloudVertex inVertex;
  public String label;
  
  public RamCloudEdge(long id, RamCloudVertex outVertex, RamCloudVertex inVertex, String label, RamCloudGraph graph) {
    // TODO Auto-generated constructor stub
    super(id, graph);
    this.outVertex = outVertex;
    this.inVertex = inVertex;
    this.label = label;
  }

  @Override
  public String getLabel() {
    // TODO Auto-generated method stub
    return label;
  }

  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    if(direction.equals(Direction.OUT))
      return outVertex;
    else if(direction.equals(Direction.IN))
      return inVertex;
    else
      throw ExceptionFactory.bothIsNotSupported();
  }

}
