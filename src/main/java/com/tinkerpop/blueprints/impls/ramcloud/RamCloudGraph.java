package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.sun.jersey.core.util.Base64;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

public class RamCloudGraph implements IndexableGraph, KeyIndexableGraph, TransactionalGraph {

  private static final Logger logger = Logger.getLogger(RamCloudGraph.class.getName());
  
  protected JRamCloud rcClient;
  
  protected long vertTableId; //(vertex_id) --> ( (n,d,ll,l), (n,d,ll,l), ... )
  protected long vertPropTableId; //(vertex_id) -> ( (kl,k,vl,v), (kl,k,vl,v), ... )
  protected long edgePropTableId; //(edge_id) -> ( (kl,k,vl,v), (kl,k,vl,v), ... )
  
  private String VERT_TABLE_NAME = "verts";
  private String EDGE_PROP_TABLE_NAME = "edge_props";
  private String VERT_PROP_TABLE_NAME = "vert_props";
  
  private static long nextVertexId = 1;
  
  private static final Features FEATURES = new Features();

  static {
    FEATURES.supportsSerializableObjectProperty = true;
    FEATURES.supportsBooleanProperty = true;
    FEATURES.supportsDoubleProperty = true;
    FEATURES.supportsFloatProperty = true;
    FEATURES.supportsIntegerProperty = true;
    FEATURES.supportsPrimitiveArrayProperty = true;
    FEATURES.supportsUniformListProperty = true;
    FEATURES.supportsMixedListProperty = true;
    FEATURES.supportsLongProperty = true;
    FEATURES.supportsMapProperty = true;
    FEATURES.supportsStringProperty = true;

    FEATURES.supportsDuplicateEdges = false;
    FEATURES.supportsSelfLoops = false;
    FEATURES.isPersistent = false;
    FEATURES.isWrapper = false;
    FEATURES.supportsVertexIteration = true;
    FEATURES.supportsEdgeIteration = true;
    FEATURES.supportsVertexIndex = false;
    FEATURES.supportsEdgeIndex = false;
    FEATURES.ignoresSuppliedIds = true;
    FEATURES.supportsTransactions = false;
    FEATURES.supportsIndices = false;
    FEATURES.supportsKeyIndices = false;
    FEATURES.supportsVertexKeyIndex = false;
    FEATURES.supportsEdgeKeyIndex = false;
    FEATURES.supportsEdgeRetrieval = true;
    FEATURES.supportsVertexProperties = true;
    FEATURES.supportsEdgeProperties = true;
    FEATURES.supportsThreadedTransactions = false;
  }
  
  static {
    System.loadLibrary("edu_stanford_ramcloud_JRamCloud");
  }
  
  public RamCloudGraph() {
    this("infrc:host=192.168.1.101,port=12246");
  }
  
  public RamCloudGraph(String coordinatorLocation) {
    this(coordinatorLocation, Level.INFO);
  }
 
  public RamCloudGraph(Level logLevel) {
    this("infrc:host=192.168.1.101,port=12246", logLevel);
  }
 
  public RamCloudGraph(String coordinatorLocation, Level logLevel) {
    logger.setLevel(logLevel);
    Handler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(logLevel);
    logger.addHandler(consoleHandler);
    logger.setUseParentHandlers(false);
    
    rcClient = new JRamCloud(coordinatorLocation);
    
    vertTableId = rcClient.createTable(VERT_TABLE_NAME);
    vertPropTableId = rcClient.createTable(VERT_PROP_TABLE_NAME);
    edgePropTableId = rcClient.createTable(EDGE_PROP_TABLE_NAME);
    
    logger.log(Level.INFO, "Connected to coordinator at " + coordinatorLocation + " and created tables " + vertTableId + ", " + vertPropTableId + ", and " + edgePropTableId);
  }

  @Override
  public Features getFeatures() {
    return FEATURES;
  }

  @Override
  public Vertex addVertex(Object id) {
    logger.log(Level.FINE, "Adding vertex: [id=" + id + "]");

    Long longId;
    
    if(id == null) {
      longId = nextVertexId++;
    } else if(id instanceof Integer) {
      longId = ((Integer) id).longValue();
    } else if(id instanceof Long) {
      longId = (Long) id;
    } else if(id instanceof String) {
      try {
        longId = Long.parseLong((String) id, 10);
      } catch(NumberFormatException e) {
        logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not a parseable long number: " + e.toString());
        return null;
      }
    } else if(id instanceof byte[]) {
      try {
        longId = ByteBuffer.wrap((byte[]) id).getLong();
      } catch(BufferUnderflowException e) {
        logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not a parseable long number: " + e.toString());
        return null;
      }
    } else {
      logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not supported. Returning null.");
      return null;
    }
    
    RamCloudVertex newVertex = new RamCloudVertex(longId, this);
    
    try {
      newVertex.create();
      return newVertex;
    } catch(IllegalArgumentException e) {
      logger.log(Level.WARNING, "Tried to create vertex " + newVertex.toString() + ": " + e.getMessage());
      return null;
    }
  }

  @Override
  public Vertex getVertex(Object id) throws IllegalArgumentException {
    Long longId;
    
    if(id == null) {
      throw ExceptionFactory.vertexIdCanNotBeNull();
    } else if(id instanceof Integer) {
      longId = ((Integer) id).longValue();
    } else if(id instanceof Long) {
      longId = (Long) id;
    } else if(id instanceof String) {
      try {
        longId = Long.parseLong((String) id, 10);
      } catch(NumberFormatException e) {
        logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not a parseable long number: " + e.toString());
        return null;
      }
    } else if(id instanceof byte[]) {
      try {
        longId = ByteBuffer.wrap((byte[]) id).getLong();
      } catch(BufferUnderflowException e) {
        logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not a parseable long number: " + e.toString());
        return null;
      }
    } else {
      logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not supported. Returning null.");
      return null;
    }
    
    RamCloudVertex vertex = new RamCloudVertex(longId, this);
    
    if(vertex.exists())
      return vertex;
    else
      return null;
  }

  @Override
  public void removeVertex(Vertex vertex) {
    logger.log(Level.FINE, "Removing vertex: [vertex=" + vertex + "]");
    
    ((RamCloudVertex) vertex).remove();
  }

  // TODO: Code review stopped here
  
  @Override
  public Iterable<Vertex> getVertices() {
    JRamCloud.TableEnumerator tableEnum = rcClient.new TableEnumerator(vertPropTableId);
    List<Vertex> vertices = new ArrayList<Vertex>();
    
    while(tableEnum.hasNext())
      vertices.add(new RamCloudVertex(tableEnum.next().key, this));
    
    return (Iterable<Vertex>)vertices;
  }

  @Override
  public Iterable<Vertex> getVertices(String key, Object value) {
    JRamCloud.TableEnumerator tableEnum = rcClient.new TableEnumerator(vertPropTableId);
    List<Vertex> vertices = new ArrayList<Vertex>();
    JRamCloud.Object tableEntry;
    
    while(tableEnum.hasNext()) {
      tableEntry = tableEnum.next();
      Map<String, Object> propMap = RamCloudElement.getPropertyMap(tableEntry.value);
      if(propMap.containsKey(key) && propMap.get(key).equals(value))
        vertices.add(new RamCloudVertex(tableEntry.key, this));
    }
    
    return (Iterable<Vertex>)vertices;
  }

  @Override
  public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) throws IllegalArgumentException {
    logger.log(Level.FINE, "Adding edge: [id=" + id + ", outVertex=" + outVertex + ", inVertex=" + inVertex + ", label=" + label + "]");
    
    if(label == null) {
      throw ExceptionFactory.edgeLabelCanNotBeNull();
    }
    
    RamCloudEdge newEdge = new RamCloudEdge((RamCloudVertex) outVertex, (RamCloudVertex) inVertex, label, this);
    
    try {
      newEdge.create();
      return newEdge;
    } catch(IllegalArgumentException e) {
      logger.log(Level.WARNING, "Tried to create edge " + newEdge.toString() + ": " + e.getMessage());
      return null;
    }
  }

  @Override
  public Edge getEdge(Object id) throws IllegalArgumentException {
    byte[] bytearrayId;
    
    if(id == null) {
      throw ExceptionFactory.edgeIdCanNotBeNull();
    } else if(id instanceof byte[]) {
      bytearrayId = (byte[]) id;
    } else if(id instanceof String) {
      bytearrayId = Base64.decode(((String) id));
    } else {
      logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is not supported. Returning null.");
      return null;
    }
    
    if(!RamCloudEdge.isValidEdgeId(bytearrayId)) {
      logger.log(Level.WARNING, "ID argument " + id.toString() + " of type " + id.getClass() + " is malformed. Returning null.");
      return null;
    }
    
    RamCloudEdge edge = new RamCloudEdge(bytearrayId, this);
    
    if(edge.exists())
      return edge;
    else 
      return null;
  }

  @Override
  public void removeEdge(Edge edge) {
    logger.log(Level.FINE, "Removing edge: [edge=" + edge + "]");
    
    edge.remove();
  }

  @Override
  public Iterable<Edge> getEdges() {
    JRamCloud.TableEnumerator tableEnum = rcClient.new TableEnumerator(edgePropTableId);
    List<Edge> edges = new ArrayList<Edge>();
    
    while(tableEnum.hasNext()) 
      edges.add(new RamCloudEdge(tableEnum.next().key, this));
    
    return (Iterable<Edge>)edges;
  }

  @Override
  public Iterable<Edge> getEdges(String key, Object value) {
    JRamCloud.TableEnumerator tableEnum = rcClient.new TableEnumerator(edgePropTableId);
    List<Edge> edges = new ArrayList<Edge>();
    JRamCloud.Object tableEntry;
    
    while(tableEnum.hasNext()) {
      tableEntry = tableEnum.next();
      Map<String, Object> propMap = RamCloudElement.getPropertyMap(tableEntry.value);
      if(propMap.containsKey(key) && propMap.get(key).equals(value))
        edges.add(new RamCloudEdge(tableEntry.key, this));
    }
    
    return (Iterable<Edge>)edges;
  }

  @Override
  public GraphQuery query() {
    return new DefaultGraphQuery(this);
  }

  @Override
  public void shutdown() {
    rcClient.dropTable(VERT_TABLE_NAME);
    rcClient.dropTable(VERT_PROP_TABLE_NAME);
    rcClient.dropTable(EDGE_PROP_TABLE_NAME);
    rcClient.disconnect();
  }

  @Override
  public void stopTransaction(Conclusion conclusion) {
    // TODO Auto-generated method stub

  }

  @Override
  public void commit() {
    // TODO Auto-generated method stub

  }

  @Override
  public void rollback() {
    // TODO Auto-generated method stub

  }

  @Override
  public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
    // TODO Auto-generated method stub

  }

  @Override
  public <T extends Element> void createKeyIndex(String key,
      Class<T> elementClass, Parameter... indexParameters) {
    // TODO Auto-generated method stub

  }

  @Override
  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends Element> Index<T> createIndex(String indexName,
      Class<T> indexClass, Parameter... indexParameters) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends Element> Index<T> getIndex(String indexName,
      Class<T> indexClass) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<Index<? extends Element>> getIndices() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void dropIndex(String indexName) {
    // TODO Auto-generated method stub

  }

  public static int count(final Iterator<?> iterator) {
    int counter = 0;
    while (iterator.hasNext()) {
        iterator.next();
        counter++;
    }
    return counter;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName().toLowerCase() + " [vertices=" + count(getVertices().iterator()) + ", edges=" + count(getEdges().iterator()) + "]";
  }

  public static void main(String[] args) {
    RamCloudGraph graph = new RamCloudGraph(Level.FINER);
    
    Vertex a = graph.addVertex(null);
    Vertex b = graph.addVertex(null);
    Vertex c = graph.addVertex(null);
    Vertex d = graph.addVertex(null);
    Vertex e = graph.addVertex(null);
    Vertex f = graph.addVertex(null);
    Vertex g = graph.addVertex(null);
    
    graph.addEdge(null, a, a, "friend");
    graph.addEdge(null, a, b, "friend1");
    graph.addEdge(null, a, b, "friend2");
    graph.addEdge(null, a, b, "friend3");
    graph.addEdge(null, a, c, "friend");
    graph.addEdge(null, a, d, "friend");
    graph.addEdge(null, a, e, "friend");
    graph.addEdge(null, a, f, "friend");
    graph.addEdge(null, a, g, "friend");
    
    graph.removeVertex(a);
    
    
    graph.shutdown();
  }

}
