package com.tinkerpop.blueprints.impls.ramcloud;

import java.util.Set;
import java.nio.ByteBuffer;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

public class RamCloudEdge implements Edge {

  private Object id;
  private RamCloudVertex outVertex;
  private RamCloudVertex inVertex;
  private String label;
  private RamCloudGraph graph;
  private byte[] rcKey;
  
  public RamCloudEdge(RamCloudVertex outVertex, RamCloudVertex inVertex, String label, RamCloudGraph graph) {
    this.outVertex = outVertex;
    this.inVertex = inVertex;
    this.label = label;
    this.graph = graph;
    
    this.rcKey = ByteBuffer.allocate(16 + label.length()).put(outVertex.getRcKey()).put(inVertex.getRcKey()).put(label.getBytes()).array();
    this.id = (Object)rcKey;
  }
  
  public RamCloudEdge(byte[] rcKey, RamCloudGraph graph) {
    ByteBuffer edgeId = ByteBuffer.wrap(rcKey);
    outVertex = new RamCloudVertex(edgeId.getLong(), graph);
    inVertex = new RamCloudVertex(edgeId.getLong(), graph);
    label = new String(rcKey, 16, rcKey.length - 16);
    this.graph = graph;
    this.rcKey = rcKey;
    this.id = (Object)rcKey;
  }

  @Override
  public <T> T getProperty(String key) {
    return graph.getProperty(this, key);
  }

  @Override
  public Set<String> getPropertyKeys() {
    return graph.getPropertyKeys(this);
  }

  @Override
  public void setProperty(String key, Object value) {
    graph.setProperty(this, key, value);
  }

  @Override
  public <T> T removeProperty(String key) {
    return graph.removeProperty(this, key);
  }

  @Override
  public void remove() {
    graph.removeEdge(this);
  }

  @Override
  public Object getId() {
    return id;
  }
  
  public byte[] getRcKey() {
    return rcKey;
  }

  public String toString() {
    return new String(outVertex.getId() + "->" + inVertex.getId() + ":" + label);
  }
  
  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    if(direction.equals(Direction.OUT))
      return outVertex;
    else if(direction.equals(Direction.IN))
      return inVertex;
    else
      throw ExceptionFactory.bothIsNotSupported();
  }

  @Override
  public String getLabel() {
    return label;
  }

}
