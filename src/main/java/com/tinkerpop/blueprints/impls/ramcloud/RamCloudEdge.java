package com.tinkerpop.blueprints.impls.ramcloud;

import java.util.Set;
import java.nio.ByteBuffer;
import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.jersey.core.util.Base64;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

public class RamCloudEdge implements Edge {

  private RamCloudVertex outVertex;
  private RamCloudVertex inVertex;
  private String label;
  private RamCloudGraph graph;
  private byte[] rcKey;
  
  private static Logger LOGGER = Logger.getLogger(RamCloudGraph.class.getName());
  
  public RamCloudEdge(RamCloudVertex outVertex, RamCloudVertex inVertex, String label, RamCloudGraph graph) {
    this.graph = graph;
    
    this.outVertex = outVertex;
    this.inVertex = inVertex;
    this.label = label;
    
    this.rcKey = ByteBuffer.allocate(16 + label.length()).put(outVertex.getRcKey()).put(inVertex.getRcKey()).put(label.getBytes()).array();
  }
  
  public RamCloudEdge(byte[] rcKey, RamCloudGraph graph) {
    this.graph = graph;
    
    ByteBuffer edgeId = ByteBuffer.wrap(rcKey);
    outVertex = new RamCloudVertex(edgeId.getLong(), graph);
    inVertex = new RamCloudVertex(edgeId.getLong(), graph);
    label = new String(rcKey, 16, rcKey.length - 16);
    
    this.rcKey = rcKey;
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
    LOGGER.log(Level.FINE, "Getting id of edge " + outVertex.getId() + "->" + inVertex.getId() + ":" + label);
    
    return (Object)new String(Base64.encode(rcKey));
  }
  
  public byte[] getRcKey() {
    LOGGER.log(Level.FINE, "Getting rcKey of edge " + outVertex.getId() + "->" + inVertex.getId() + ":" + label);
    return rcKey;
  }

  public String toString() {
    return new String(outVertex.getId() + "->" + inVertex.getId() + ":" + label);
  }
  
  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    LOGGER.log(Level.FINE, "Getting " + direction.toString() + " vertex of edge " + outVertex.getId() + "->" + inVertex.getId() + ":" + label);
    
    if(direction.equals(Direction.OUT))
      return outVertex;
    else if(direction.equals(Direction.IN))
      return inVertex;
    else
      throw ExceptionFactory.bothIsNotSupported();
  }

  @Override
  public String getLabel() {
    LOGGER.log(Level.FINE, "Getting label of edge: " + label);
    return label;
  }

}
