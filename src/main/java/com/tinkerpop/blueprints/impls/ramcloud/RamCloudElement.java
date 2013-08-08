package com.tinkerpop.blueprints.impls.ramcloud;

import java.util.Set;

import com.tinkerpop.blueprints.Element;

abstract class RamCloudElement implements Element {

  public Long id;
  private RamCloudGraph graph;
  
  public RamCloudElement(long id, RamCloudGraph graph) {
    this.id = id;
    this.graph = graph;
  }
  
  @Override
  public Object getId() {
    // TODO Auto-generated method stub
    return id;
  }

  @Override
  public <T> T getProperty(String key) {
    // TODO Auto-generated method stub
    return (T) graph.getProperty(id, key);
  }

  @Override
  public Set<String> getPropertyKeys() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void remove() {
    // TODO Auto-generated method stub

  }

  @Override
  public <T> T removeProperty(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setProperty(String key, Object value) {
    // TODO Auto-generated method stub
    graph.setProperty(id, key, value);
  }

}
