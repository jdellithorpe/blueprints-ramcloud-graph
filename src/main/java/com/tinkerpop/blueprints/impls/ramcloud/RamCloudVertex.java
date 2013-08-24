package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;

import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

import com.tinkerpop.blueprints.util.DefaultVertexQuery;

public class RamCloudVertex implements Vertex {

  private long id;
  private RamCloudGraph graph;
  private byte[] rcKey;
  
  private static Logger LOGGER = Logger.getLogger(RamCloudGraph.class.getName());
  
  public RamCloudVertex(byte[] rcKey, RamCloudGraph graph) {
    this.rcKey = rcKey;
    this.graph = graph;

    this.id = ByteBuffer.wrap(rcKey).getLong();
  }
  
  public RamCloudVertex(long id, RamCloudGraph graph) {
    this.id = id;
    this.graph = graph;
    
    this.rcKey = ByteBuffer.allocate(8).putLong(this.id).array();
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
    graph.removeVertex(this);
  }

  @Override
  public Object getId() {
    LOGGER.log(Level.FINE, "Getting id of vertex " + id);
    return (Object)id;
  }
  
  public byte[] getRcKey() {
    LOGGER.log(Level.FINE, "Getting rcKey of vertex " + id);
    return rcKey;
  }
  
  @Override
  public Iterable<Edge> getEdges(Direction direction, String... labels) {
    return graph.getEdges(this, direction, labels);
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, String... labels) {
    LOGGER.log(Level.FINE, "Getting neighbor vertices of vertex " + id + " in direction " + direction.toString());
    Iterator<Edge> edges = getEdges(direction, labels).iterator();
    
    ArrayList<Vertex> vertexArray = new ArrayList<Vertex>();
    
    while(edges.hasNext()) {
      RamCloudEdge edge = (RamCloudEdge)edges.next();
      if(edge.getVertex(Direction.OUT).equals(this)) {
        vertexArray.add(edge.getVertex(Direction.IN));
      } else if(edge.getVertex(Direction.IN).equals(this)) {
        vertexArray.add(edge.getVertex(Direction.OUT));
      } else {
        return null;
      }
    }
    
    return (Iterable<Vertex>)vertexArray;
  }

  @Override
  public VertexQuery query() {
    return new DefaultVertexQuery(this);
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex) {
    return graph.addEdge(null, this, inVertex, label);
  }
  
  public String toString() {
    return new String("Vertex " + id);
  }
}
