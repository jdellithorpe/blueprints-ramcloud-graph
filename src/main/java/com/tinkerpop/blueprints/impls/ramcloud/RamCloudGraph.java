package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;
import java.util.concurrent.atomic.*;

import edu.stanford.ramcloud.JRamCloud;

public class RamCloudGraph implements Graph {
  /* TODO
   *  - The ByteBuffer class is used to parse the values stored in RAMCloud, 
   *  and when multiple clients are involved in reading/writing values, 
   *  byte order is going to be important, but at the moment we leave it to 
   *  every machine's default. We need to explicitly specify this.
   */
  
  static {
    System.loadLibrary("edu_stanford_ramcloud_JRamCloud");
  }
  
  private static Logger LOGGER = Logger.getLogger(RamCloudGraph.class.getName());
  
  private JRamCloud ramCloudClient;
  
  private long vertTableId; //(vertex_id) --> ( (n,d,ll,l), (n,d,ll,l), ... )
  private long edgePropTableId; //(edge_id) -> ( (kl,k,vl,v), (kl,k,vl,v), ... )
  private long vertPropTableId; //(vertex_id) -> ( (kl,k,vl,v), (kl,k,vl,v), ... )
  
  private String VERT_TABLE_NAME = "verts";
  private String EDGE_PROP_TABLE_NAME = "edge_props";
  private String VERT_PROP_TABLE_NAME = "vert_props";
  
  public RamCloudGraph(String coordinatorLocation, Level logLevel) {
    LOGGER.setLevel(logLevel);
    Handler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(logLevel);
    LOGGER.addHandler(consoleHandler);
    
    ramCloudClient = new JRamCloud(coordinatorLocation);
    LOGGER.log(Level.FINE,  "Connected to RAMCloud Coordinator at " + coordinatorLocation);
    
    vertTableId = ramCloudClient.createTable(VERT_TABLE_NAME);
    edgePropTableId = ramCloudClient.createTable(EDGE_PROP_TABLE_NAME);
    vertPropTableId = ramCloudClient.createTable(VERT_PROP_TABLE_NAME);
  }

  @Override
  public Features getFeatures() {
    Features feat = new Features();
    feat.isPersistent = true;
    return feat;
  }

  @Override
  public Vertex addVertex(Object id) {
    /* TODO
     *  - Presently we don't check if the vertex already exists. In this case,
     *  the vertex properties are deleted, and the edge list for the vertex is
     *  erased, possibly leaving dangling edges. Since this safety check is 
     *  not presently made, we are depending on the client to never make the 
     *  mistake of creating a vertex that already exists.
     */
    /*
     * Logical steps:
     *  - Create a new RamCloudVertex object
     *  - Create the vertex in RAMCloud memory
     *   - Put an empty entry in the vertex table
     *   - Put an empty entry in the vertex property table
     */
    
    LOGGER.log(Level.FINE, "Creating new vertex with id: " + (Long)id);
    RamCloudVertex newVertex = new RamCloudVertex((Long)id, this);
    
    LOGGER.log(Level.FINER, "Writing blank entry to the vertex table");
    ramCloudClient.write(vertTableId, newVertex.getRcKey(), ByteBuffer.allocate(0).array());
    LOGGER.log(Level.FINER, "Writing blank entry to the vertex property table");
    ramCloudClient.write(vertPropTableId, newVertex.getRcKey(), ByteBuffer.allocate(0).array());
    
    return newVertex;
  }

  @Override
  public Vertex getVertex(Object id) {
    /*
     * Logical steps:
     *  - Create RamCloudVertex object
     *  - Check to see if the vertex is in ramcloud memory
     *   - If yes then return the vertex object
     *   - If no then return null
     */
    
    RamCloudVertex vertex = new RamCloudVertex((Long)id, this);
    
    LOGGER.log(Level.FINE, "Reading vertex with id: " + (Long)id);
    
    try {
      ramCloudClient.read(vertTableId, vertex.getRcKey());
      LOGGER.log(Level.FINER, "Found vertex " + (Long)id + " in RAMCloud memory");
      return vertex;
    } catch(Exception e) {
      LOGGER.log(Level.FINER, "Did not find vertex " + (Long)id + " in RAMCloud memory");
      return null;
    }
  }

  @Override
  public void removeVertex(Vertex vertex) {
    /* TODO
     *  - Currently the algorithm removes the edges from both this vertex and also
     *  from all neighbor vertices' edge lists, but only the latter is necessary
     *  since this vertex is completely deleted. A further optimization would be 
     *  to group the edges by neighbor vertex, and delete them in batches, which 
     *  would reduce the total number of RAMCloud access to 1 read and 1 write for 
     *  each neighbor vertex, 1 delete on this vertex, and 1 delete on the props.
     *  But presently, edges are deleted one by one, and this is not efficient.
     *   - Check to see what happens in error cases (i.e. the vertex does not 
     *   exist) and make sure that's handled correctly.
     */
    // Remove all edges for which this vertex is an endpoint
    Iterator<Edge> edges = getEdges((RamCloudVertex)vertex, Direction.BOTH).iterator();
    while(edges.hasNext()) {
      RamCloudEdge edge = (RamCloudEdge)edges.next();
      removeEdge(edge);
    }
    
    // Remove the vertex from the vertex table
    ramCloudClient.remove(vertTableId, ((RamCloudVertex)vertex).getRcKey());
    
    // Remove the vertex from the vertex properties table
    ramCloudClient.remove(vertPropTableId, ((RamCloudVertex)vertex).getRcKey());
  }

  @Override
  public Iterable<Vertex> getVertices() {
    LOGGER.log(Level.FINE, "Getting all the vertices in the graph...");
    JRamCloud.TableEnumerator tableEnum = ramCloudClient.new TableEnumerator(vertPropTableId);
    
    ArrayList<Vertex> vertArray = new ArrayList<Vertex>();
    
    while(tableEnum.hasNext())
      vertArray.add(new RamCloudVertex(tableEnum.next().key, this));
    
    return (Iterable<Vertex>)vertArray;
  }

  @Override
  public Iterable<Vertex> getVertices(String key, Object value) {
    LOGGER.log(Level.FINE, "Getting all the vertices in the graph with (key="+key+",value="+value+")...");
    JRamCloud.TableEnumerator tableEnum = ramCloudClient.new TableEnumerator(vertPropTableId);
    
    ArrayList<Vertex> vertArray = new ArrayList<Vertex>();
    JRamCloud.Object tableEntry;
    
    while(tableEnum.hasNext()) {
      tableEntry = tableEnum.next();
      HashMap<String, Object> propMap = new HashMap<String, Object>();
      deserializeProperties(tableEntry.value, propMap);
      if(propMap.containsKey(key) && propMap.get(key).equals(value))
        vertArray.add(new RamCloudVertex(tableEntry.key, this));
    }
    
    return (Iterable<Vertex>)vertArray;
  }

  @Override
  public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
    /* TODO
     *  - Might want to check to make sure the vertices already exists
     *  - Might want to check to see if the edge already exists between the two vertices
     *  - Return the proper exception in the event of an error
     */
    /*
     * Logical steps:
     *  - Create a new RamCloudEdge object
     *  - Create the edge in RAMCloud memory
     *   - Insert edge in the outVertex's vertex table entry
     *   - Insert edge in the inVertex's vertex table entry
     *   - Put an empty entry in the edge property table
     */
    
    LOGGER.log(Level.FINE, "Creating edge " + outVertex.getId() + "->" + inVertex.getId() + ":" + label);
    RamCloudEdge newEdge = new RamCloudEdge((RamCloudVertex)outVertex, (RamCloudVertex)inVertex, label, this);
    
    LOGGER.log(Level.FINER, "Writing blank entry to the edge property table");
    ramCloudClient.write(edgePropTableId, newEdge.getRcKey(), ByteBuffer.allocate(0).array());
    
    LOGGER.log(Level.FINER, "Writing edge to outVertex...");
    JRamCloud.Object rcEntry = ramCloudClient.read(vertTableId, ((RamCloudVertex)outVertex).getRcKey());
    byte[] newRcValue = ByteBuffer.allocate(rcEntry.value.length + 8 + 1 + 2 + label.length()).put(rcEntry.value).put(((RamCloudVertex)inVertex).getRcKey()).put(directionToByteCode(Direction.OUT)).putShort((short)label.length()).put(label.getBytes()).array();
    ramCloudClient.write(vertTableId, ((RamCloudVertex)outVertex).getRcKey(), newRcValue);
    
    LOGGER.log(Level.FINER, "Writing edge to inVertex...");
    rcEntry = ramCloudClient.read(vertTableId, ((RamCloudVertex)inVertex).getRcKey());
    newRcValue = ByteBuffer.allocate(rcEntry.value.length + 8 + 1 + 2 + label.length()).put(rcEntry.value).put(((RamCloudVertex)outVertex).getRcKey()).put(directionToByteCode(Direction.IN)).putShort((short)label.length()).put(label.getBytes()).array();
    ramCloudClient.write(vertTableId, ((RamCloudVertex)inVertex).getRcKey(), newRcValue);
    
    return newEdge;
  }
  
  private byte directionToByteCode(Direction direction) {
    switch(direction) {
    case OUT:
      return 1;
    case IN:
      return 2;
    case BOTH:
      return 3;
    default:
      return 0;
    }
  }
  
  private Direction byteCodeToDirection(byte code) {
    switch(code) {
    case 1:
      return Direction.OUT;
    case 2:
      return Direction.IN;
    case 3:
      return Direction.BOTH;
    default:
      return null;
    }
  }

  @Override
  public Edge getEdge(Object id) {
    /* TODO
     *  - I'm assuming right now that the edge actually exists... but we
     *  might want to do a check for that and return null or throw an 
     *  exception in the case that the edge actually doesn't exist in 
     *  RAMCloud.
     */
    return new RamCloudEdge((byte[])id, this);
  }

  @Override
  public void removeEdge(Edge edge) {
    /* TODO
     *  - It's possible that the edge doesn't exist, in which case nothing bad will happen.
     */
    RamCloudVertex outVertex = (RamCloudVertex)edge.getVertex(Direction.OUT);
    RamCloudVertex inVertex = (RamCloudVertex)edge.getVertex(Direction.IN);
    byte[] rcKey = ((RamCloudEdge)edge).getRcKey();
    
    LOGGER.log(Level.FINE, "Removing edge: " + outVertex.getId() + "->" + inVertex.getId() + ":" + edge.getLabel());
    
    ramCloudClient.remove(edgePropTableId, rcKey);
    LOGGER.log(Level.FINER, "Removed property table entry");
    
    JRamCloud.Object vertTableEntry = ramCloudClient.read(vertTableId, outVertex.getRcKey());
    
    ByteBuffer edges = ByteBuffer.wrap(vertTableEntry.value);
    boolean found = false;
    long edgeNeighborId;
    Direction edgeDirection;
    short labelLength = 0;
    String label;
    while(edges.remaining() > 0) {
      edgeNeighborId = edges.getLong();
      edgeDirection = byteCodeToDirection(edges.get());
      labelLength = edges.getShort();
      label = new String(edges.array(), edges.position(), labelLength);
      edges.position(edges.position() + labelLength);
        
      if((edgeDirection.equals(Direction.OUT)) && (edgeNeighborId == (Long)inVertex.getId()) && (label.equals(edge.getLabel()))) {
        found = true;
        break;
      }
    }
    
    if(found) {
      // The position of the ByteBuffer rests at the beginning of the next edge, if any
      LOGGER.log(Level.FINER, "Found edge in outVertex");
      byte[] src = vertTableEntry.value;
      int segmentLength = 8 + 1 + 2 + labelLength;
      int newCapacity = src.length - segmentLength;
      int snipStart = edges.position() - segmentLength;
      int snipEnd = edges.position();
      byte[] newVertTableEntry = ByteBuffer.allocate(newCapacity).put(src, 0, snipStart).put(src, snipEnd, src.length - snipEnd).array();
      ramCloudClient.write(vertTableId, outVertex.getRcKey(), newVertTableEntry);
    }
    
    vertTableEntry = ramCloudClient.read(vertTableId, inVertex.getRcKey());
    
    edges = ByteBuffer.wrap(vertTableEntry.value);
    found = false;
    while(edges.remaining() > 0) {
      edgeNeighborId = edges.getLong();
      edgeDirection = byteCodeToDirection(edges.get());
      labelLength = edges.getShort();
      label = new String(edges.array(), edges.position(), labelLength);
      edges.position(edges.position() + labelLength);
        
      if((edgeDirection.equals(Direction.IN)) && (edgeNeighborId == (Long)outVertex.getId()) && (label.equals(edge.getLabel()))) {
        found = true;
        break;
      }
    }
    
    if(found) {
      // The position of the ByteBuffer rests at the beginning of the next edge, if any
      LOGGER.log(Level.FINER, "Found edge in inVertex");
      byte[] src = vertTableEntry.value;
      int segmentLength = 8 + 1 + 2 + labelLength;
      int newCapacity = src.length - segmentLength;
      int snipStart = edges.position() - segmentLength;
      int snipEnd = edges.position();
      byte[] newVertTableEntry = ByteBuffer.allocate(newCapacity).put(src, 0, snipStart).put(src, snipEnd, src.length - snipEnd).array();
      ramCloudClient.write(vertTableId, inVertex.getRcKey(), newVertTableEntry);
    }
    
  }

  @Override
  public Iterable<Edge> getEdges() {
    LOGGER.log(Level.FINE, "Getting all the edges in the graph...");
    JRamCloud.TableEnumerator tableEnum = ramCloudClient.new TableEnumerator(edgePropTableId);
    
    ArrayList<Edge> edgeArray = new ArrayList<Edge>();
    
    while(tableEnum.hasNext())
      edgeArray.add(new RamCloudEdge(tableEnum.next().key, this));
    
    return (Iterable<Edge>)edgeArray;
  }

  @Override
  public Iterable<Edge> getEdges(String key, Object value) {
    LOGGER.log(Level.FINE, "Getting all the vertices in the graph with (key="+key+",value="+value+")...");
    JRamCloud.TableEnumerator tableEnum = ramCloudClient.new TableEnumerator(edgePropTableId);
    
    ArrayList<Edge> edgeArray = new ArrayList<Edge>();
    JRamCloud.Object tableEntry;
    
    while(tableEnum.hasNext()) {
      tableEntry = tableEnum.next();
      HashMap<String, Object> propMap = new HashMap<String, Object>();
      deserializeProperties(tableEntry.value, propMap);
      if(propMap.containsKey(key) && propMap.get(key).equals(value))
        edgeArray.add(new RamCloudEdge(tableEntry.key, this));
    }
    
    return (Iterable<Edge>)edgeArray;
  }
  
  private Iterable<Edge> parseVertexTableEntry(byte[] key, byte[] value) {
    ByteBuffer edges = ByteBuffer.wrap(value);
    ArrayList<Edge> edgeArray = new ArrayList<Edge>();
    
    long vertexId = ByteBuffer.wrap(key).getLong();
    RamCloudVertex vertex = new RamCloudVertex(vertexId, this);
    
    LOGGER.log(Level.FINE, "Parsing vertex table entry for vertex " + vertexId);
    
    while(edges.remaining() > 0) {
      long edgeNeighborId = edges.getLong();
      Direction edgeDirection = byteCodeToDirection(edges.get());
      short labelLength = edges.getShort();
      String label = new String(edges.array(), edges.position(), labelLength);
      edges.position(edges.position() + labelLength);
      
      LOGGER.log(Level.FINER, "Edge: (" + edgeNeighborId + ", " + edgeDirection.toString() + ", " + label + ")");
      
      if(edgeDirection == Direction.OUT) {
        edgeArray.add(new RamCloudEdge(vertex, new RamCloudVertex(edgeNeighborId, this), label, this));
      } else if(edgeDirection == Direction.IN) {
        edgeArray.add(new RamCloudEdge(new RamCloudVertex(edgeNeighborId, this), vertex, label, this));
      } else {
        LOGGER.log(Level.SEVERE, "Read edge (" + edgeNeighborId + ", " + edgeDirection.toString() + ", " + label + ") but the edge direction is not supported");
        return null;
      }
    }
    
    LOGGER.log(Level.FINER,  "Finished parsing vertex table entry for vertex " + vertex.getId());
    
    return (Iterable<Edge>) edgeArray;
  }
  
  public Iterable<Edge> getEdges(RamCloudVertex vertex, Direction direction, String... labels) {
    /* TODO
     *  - In the event that the vertex does not exist, this function will simply return an empty
     *  list. We might want to throw an exception in this case, however.
     */
    JRamCloud.Object vertTableEntry = ramCloudClient.read(vertTableId, vertex.getRcKey());
    ByteBuffer edges = ByteBuffer.wrap(vertTableEntry.value);
    ArrayList<Edge> edgeArray = new ArrayList<Edge>();
    
    LOGGER.log(Level.FINE, "Reading edges for vertex " + vertex.getId());
    
    while(edges.remaining() > 0) {
      long edgeNeighborId = edges.getLong();
      Direction edgeDirection = byteCodeToDirection(edges.get());
      short labelLength = edges.getShort();
      String label = new String(edges.array(), edges.position(), labelLength);
      edges.position(edges.position() + labelLength);
      
      LOGGER.log(Level.FINER, "Edge: (" + edgeNeighborId + ", " + edgeDirection.toString() + ", " + label + ")");
      
      if((direction == Direction.BOTH || edgeDirection.equals(direction)) && (labels.length == 0 || Arrays.asList(labels).contains(label))) {
        if(edgeDirection == Direction.OUT) {
          edgeArray.add(new RamCloudEdge(vertex, new RamCloudVertex(edgeNeighborId, this), label, this));
        } else if(edgeDirection == Direction.IN) {
          edgeArray.add(new RamCloudEdge(new RamCloudVertex(edgeNeighborId, this), vertex, label, this));
        } else {
          LOGGER.log(Level.SEVERE, "Read edge (" + edgeNeighborId + ", " + edgeDirection.toString() + ", " + label + ") but the edge direction is not supported");
          return null;
        }
      }
    }
    
    LOGGER.log(Level.FINER,  "Finished reading edges for vertex " + vertex.getId());
    
    return (Iterable<Edge>) edgeArray;
  }

  private static byte[] serializeProperties(HashMap<String, Object> values) {
      byte[][] kvArray = new byte[values.size() * 2][];

      int kvIndex = 0;
      int totalLength = 0;
      for (Map.Entry<String, Object> entry : values.entrySet()) {
          totalLength += 8;   // fields denoting key length and value length

          byte[] keyBytes = entry.getKey().getBytes();
          kvArray[kvIndex++] = keyBytes;
          totalLength += keyBytes.length;

          byte[] valueBytes = entry.getValue().toString().getBytes();
          kvArray[kvIndex++] = valueBytes;
          totalLength += valueBytes.length;
      }
      ByteBuffer buf = ByteBuffer.allocate(totalLength);
      buf.order(ByteOrder.LITTLE_ENDIAN);

      kvIndex = 0;
      for (int i = 0; i < kvArray.length / 2; i++) {
          byte[] keyBytes = kvArray[kvIndex++];
          buf.putInt(keyBytes.length);
          buf.put(keyBytes);
          byte[] valueBytes = kvArray[kvIndex++];
          buf.putInt(valueBytes.length);
          buf.put(valueBytes);
      }

      return buf.array();
  }

  private static void deserializeProperties(byte[] bytes, HashMap<String, Object> into) {
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      buf.order(ByteOrder.LITTLE_ENDIAN);

      while (buf.remaining() > 0) {
        int keyByteLength = buf.getInt();
        String key = new String(bytes, buf.position(), keyByteLength);
        buf.position(buf.position() + keyByteLength);

        int valueByteLength = buf.getInt();
        String value = new String(bytes, buf.position(), valueByteLength);
        buf.position(buf.position() + valueByteLength);

        into.put(key, (Object)value);
      }
  }

  public <T> T getProperty(Element element, String key) {
    /* TODO
     *  - Robustify this function to handle elements that do not exist
     */
    byte[] rcKey;
    long tableId;
    HashMap<String, Object> propMap = new HashMap<String, Object>();
    
    if(element instanceof RamCloudVertex) {
      RamCloudVertex vertex = (RamCloudVertex)element;
      rcKey = vertex.getRcKey();
      tableId = vertPropTableId;
    } else if(element instanceof RamCloudEdge) {
      RamCloudEdge edge = (RamCloudEdge)element;
      rcKey = edge.getRcKey();
      tableId = edgePropTableId;
    } else {
      LOGGER.log(Level.SEVERE, "Setting property on element that is neither a vertex nor an edge");
      return null;
    }
    
    JRamCloud.Object propTableEntry = ramCloudClient.read(tableId, rcKey);
    deserializeProperties(propTableEntry.value, propMap);
    
    return (T)propMap.get(key);
  }

  public Set<String> getPropertyKeys(Element element) {
    /* TODO
     *  - Robustify this function to handle elements that do not exist
     */
    byte[] rcKey;
    long tableId;
    HashMap<String, Object> propMap = new HashMap<String, Object>();
    
    if(element instanceof RamCloudVertex) {
      RamCloudVertex vertex = (RamCloudVertex)element;
      rcKey = vertex.getRcKey();
      tableId = vertPropTableId;
    } else if(element instanceof RamCloudEdge) {
      RamCloudEdge edge = (RamCloudEdge)element;
      rcKey = edge.getRcKey();
      tableId = edgePropTableId;
    } else {
      LOGGER.log(Level.SEVERE, "Setting property on element that is neither a vertex nor an edge");
      return null;
    }
    
    JRamCloud.Object propTableEntry = ramCloudClient.read(tableId, rcKey);
    deserializeProperties(propTableEntry.value, propMap);
    
    return propMap.keySet();
  }

  public void setProperty(Element element, String key, Object value) {
    /* TODO
     *  - Robustify this function to handle elements that do not exist
     */
    byte[] rcKey;
    long tableId;
    HashMap<String, Object> propMap = new HashMap<String, Object>();
    
    if(element instanceof RamCloudVertex) {
      RamCloudVertex vertex = (RamCloudVertex)element;
      rcKey = vertex.getRcKey();
      tableId = vertPropTableId;
    } else if(element instanceof RamCloudEdge) {
      RamCloudEdge edge = (RamCloudEdge)element;
      rcKey = edge.getRcKey();
      tableId = edgePropTableId;
    } else {
      LOGGER.log(Level.SEVERE, "Setting property on element that is neither a vertex nor an edge");
      return;
    }
    
    JRamCloud.Object propTableEntry = ramCloudClient.read(tableId, rcKey);
    deserializeProperties(propTableEntry.value, propMap);
    propMap.put(key, value);
    byte[] newRcValue = serializeProperties(propMap);
    ramCloudClient.write(tableId, rcKey, newRcValue);
  }

  public <T> T removeProperty(Element element, String key) {
    /* TODO
     *  - Robustify this function to handle elements that do not exist
     */
    byte[] rcKey;
    long tableId;
    T oldValue;
    HashMap<String, Object> propMap = new HashMap<String, Object>();
    
    if(element instanceof RamCloudVertex) {
      RamCloudVertex vertex = (RamCloudVertex)element;
      rcKey = vertex.getRcKey();
      tableId = vertPropTableId;
    } else if(element instanceof RamCloudEdge) {
      RamCloudEdge edge = (RamCloudEdge)element;
      rcKey = edge.getRcKey();
      tableId = edgePropTableId;
    } else {
      LOGGER.log(Level.SEVERE, "Setting property on element that is neither a vertex nor an edge");
      return null;
    }
    
    JRamCloud.Object propTableEntry = ramCloudClient.read(tableId, rcKey);
    deserializeProperties(propTableEntry.value, propMap);
    oldValue = (T)propMap.remove(key);
    byte[] newRcValue = serializeProperties(propMap);
    ramCloudClient.write(tableId, rcKey, newRcValue);
    return oldValue;
  }
  
  @Override
  public GraphQuery query() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub

  }
  
  public static void main(String[] args) {
    RamCloudGraph rcgraph = new RamCloudGraph("infrc:host=192.168.1.101,port=12246", Level.FINER);
    
    Vertex chenchen = rcgraph.addVertex((long)1);
    Vertex kate = rcgraph.addVertex((long)2);
    Vertex sophia = rcgraph.addVertex((long)3);
    Vertex jonathan = rcgraph.addVertex((long)4);
    Vertex kyle = rcgraph.addVertex((long)5);
    Vertex eddie = rcgraph.addVertex((long)6);
    
    rcgraph.addEdge(null, chenchen, kate, "wife");
    rcgraph.addEdge(null, kate, chenchen, "husband");
    Edge edge0 = rcgraph.addEdge(null, jonathan, sophia, "girlfriend");
    Edge edge1 = rcgraph.addEdge(null, sophia, jonathan, "boyfriend");
    Edge edge2 = rcgraph.addEdge(null, sophia, chenchen, "friend");
    Edge edge3 = rcgraph.addEdge(null, sophia, kate, "friend");
    Edge edge4 = rcgraph.addEdge(null, jonathan, kyle, "bro");
    rcgraph.addEdge(null, kyle, eddie, "pet");
    rcgraph.addEdge(null, eddie, kyle, "master");
    
    Iterator<Edge> edges = rcgraph.getEdges((RamCloudVertex)jonathan, Direction.BOTH).iterator();
    while(edges.hasNext()) {
      RamCloudEdge edge = (RamCloudEdge)edges.next();
      System.out.println(edge.toString());
    }
    
    jonathan.setProperty("name", "jonathan");
    jonathan.setProperty("age", 27);
    System.out.println(jonathan.getProperty("name"));
    System.out.println(jonathan.getProperty("age"));
    
    edge0.setProperty("firstdate", "20130329");
    
    /*
    rcgraph.removeVertex(kyle);
    
    edges = rcgraph.getEdges((RamCloudVertex)jonathan, Direction.BOTH).iterator();
    while(edges.hasNext()) {
      RamCloudEdge edge = (RamCloudEdge)edges.next();
      System.out.println(edge.toString());
    }
    */
    
    edges = rcgraph.getEdges().iterator();
    while(edges.hasNext()) {
      RamCloudEdge edge = (RamCloudEdge)edges.next();
      System.out.println(edge.toString());
    }
    
    Iterator<Vertex> verts = rcgraph.getVertices("age", "27").iterator();
    while(verts.hasNext()) {
      RamCloudVertex vert = (RamCloudVertex)verts.next();
      System.out.println(vert.toString());
    }
    
    edges = rcgraph.getEdges("firstdate", "20130329").iterator();
    while(edges.hasNext()) {
      RamCloudEdge edge = (RamCloudEdge)edges.next();
      System.out.println(edge.toString());
    }
  }
}
