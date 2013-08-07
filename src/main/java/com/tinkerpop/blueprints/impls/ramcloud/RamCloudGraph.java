package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

public class RamCloudGraph implements Graph {

  static {
    System.loadLibrary("edu_stanford_ramcloud_JRamCloud");
  }
  
  private JRamCloud ramcloud;
  private long prop_table_id; //(id, propkey) --> (propvalue)
  private long edge_table_id; //(id) --> (outVertexId, inVertexId, label)
  private String PROP_TABLE_NAME = "props";
  private String EDGE_TABLE_NAME = "edges";
  
  public RamCloudGraph(String coordinator_location) {
    // TODO Auto-generated constructor stub
    ramcloud = new JRamCloud(coordinator_location);
    prop_table_id = ramcloud.createTable(PROP_TABLE_NAME);
    edge_table_id = ramcloud.createTable(EDGE_TABLE_NAME);
    System.out.println("prop_table_id = " + prop_table_id);
    System.out.println("edge_table_id = " + edge_table_id);
  }

  @Override
  public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
    // TODO Auto-generated method stub
    if(id == null)
      throw ExceptionFactory.edgeIdCanNotBeNull();
    
    if(label == null)
      throw ExceptionFactory.edgeLabelCanNotBeNull();
    
    RamCloudEdge edge = new RamCloudEdge((long)id, (RamCloudVertex)outVertex, (RamCloudVertex)inVertex, label, this);
    
    ramCloudWriteEdge(edge);
      
    return edge;
  }

  @Override
  public Vertex addVertex(Object id) {
    // TODO Auto-generated method stub
    if(id == null)
      throw ExceptionFactory.vertexIdCanNotBeNull();
    
    return new RamCloudVertex((long)id, this);
  }

  
  public String getProperty(Object id, String key) {
    byte[] rckey = ByteBuffer.allocate(8 + key.length()).putLong((long)id).put(key.getBytes()).array();
    
    JRamCloud.Object o = ramcloud.read(prop_table_id, rckey);
    
    return o.getValue();
  }
  
  public void setProperty(Object id, String key, Object value) {
    byte[] rckey = ByteBuffer.allocate(8 + key.length()).putLong((long)id).put(key.getBytes()).array();
    byte[] rcvalue = ((String)value).getBytes();
    
    ramcloud.write(prop_table_id, rckey, rcvalue);
  }
  
  @Override
  public Edge getEdge(Object id) {
    // TODO Auto-generated method stub
    byte[] key = ByteBuffer.allocate(8).putLong((long)id).array();    
    
    JRamCloud.Object o = ramcloud.read(edge_table_id, key);
    
    ByteBuffer value = ByteBuffer.wrap(o.value);
    long outVertexId = value.getLong(0);
    long inVertexId = value.getLong(8);
    String label = new String(value.array(), value.position() + 16, value.remaining() - 16);
    
    // Debug
    //System.out.println("reading key = " + Arrays.toString(key));
    //System.out.println("reading value = " + Arrays.toString(o.value));
    //System.out.println("outVertexId = " + outVertexId);
    //System.out.println("inVertexId = " + inVertexId);
    //System.out.println("label = " + label);
    
    return new RamCloudEdge((long)id, new RamCloudVertex(outVertexId, this), new RamCloudVertex(inVertexId, this), label, this);
  }

  private void ramCloudWriteEdge(RamCloudEdge edge) {
    byte[] key = ByteBuffer.allocate(8).putLong((long)edge.getId()).array();
    byte[] value = ByteBuffer.allocate(16 + edge.label.length()).putLong((long)edge.outVertex.getId()).putLong((long)edge.inVertex.getId()).put(edge.label.getBytes()).array();
    
    // Debug
    //System.out.println("writing key = " + Arrays.toString(key));
    //System.out.println("writing value = " + Arrays.toString(value));
    
    ramcloud.write(edge_table_id, key, value);
  }
  
  @Override
  public Iterable<Edge> getEdges() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<Edge> getEdges(String arg0, Object arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Features getFeatures() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Vertex getVertex(Object id) {
    // TODO Auto-generated method stub
    return new RamCloudVertex((long)id, this);
  }

  @Override
  public Iterable<Vertex> getVertices() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<Vertex> getVertices(String arg0, Object arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GraphQuery query() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void removeEdge(Edge arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeVertex(Vertex arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub

  }


  public static void main(String[] args) {
    RamCloudGraph rcgraph = new RamCloudGraph("infrc:host=192.168.1.109,port=12246");
    
    Vertex bob = rcgraph.addVertex((long)1);
    Vertex alice = rcgraph.addVertex((long)2);
    rcgraph.addEdge((long)3, bob, alice, "married");
    Edge edge = rcgraph.getEdge((long)3);
    
    System.out.println("id: " + edge.getId());
    System.out.println("outVertexId: " + edge.getVertex(Direction.OUT).getId());
    System.out.println("inVertexId: " + edge.getVertex(Direction.IN).getId());
    System.out.println("label: " + edge.getLabel());
    
    bob.setProperty("name", "bob");
    bob.setProperty("occupation", "barristo");
    
    System.out.println("bob's name: " + bob.getProperty("name"));
    System.out.println("bob's job: " + bob.getProperty("occupation"));
  }

}
