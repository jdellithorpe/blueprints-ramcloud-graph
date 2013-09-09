package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.ramcloud.RamCloudGraphProtos.EdgeListProtoBuf;
import com.tinkerpop.blueprints.impls.ramcloud.RamCloudGraphProtos.EdgeProtoBuf;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

public class RamCloudVertex extends RamCloudElement implements Vertex {

  private static final Logger logger = Logger.getLogger(RamCloudGraph.class.getName());
  
  protected long id;
  protected byte[] rcKey;
  private RamCloudGraph graph;
  
  /*
   * Constructors
   */
  public RamCloudVertex(long id, RamCloudGraph graph) {
    super(idToRcKey(id), graph.vertPropTableId, graph.rcClient);
    
    this.id = id;
    this.rcKey = idToRcKey(id);
    this.graph = graph;
  }

  public RamCloudVertex(byte[] rcKey, RamCloudGraph graph) {
    super(rcKey, graph.vertPropTableId, graph.rcClient);
    
    this.id = rcKeyToId(rcKey);
    this.rcKey = rcKey;
    this.graph = graph;
  }

  /*
   * Vertex interface implementation
   */
  @Override
  public Edge addEdge(String label, Vertex inVertex) {
    return graph.addEdge(null, this, inVertex, label);
  }
  
  @Override
  public Iterable<Edge> getEdges(Direction direction, String... labels) {
    return new ArrayList<Edge>(getEdgeList(direction, labels));
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, String... labels) {
    List<RamCloudEdge> edges = getEdgeList(direction, labels);
    List<Vertex> neighbors = new ArrayList<Vertex>();
    for(RamCloudEdge edge: edges) {
      neighbors.add(edge.getNeighbor(this));
    }
    return neighbors;
  }

  @Override
  public VertexQuery query() {
    return new DefaultVertexQuery(this);
  }
  
  /*
   * RamCloudElement overridden methods
   */
  @Override
  public Object getId() {
    return id;
  }

  @Override
  public void remove() {
    Set<RamCloudEdge> edges = getEdgeSet();
    Map<RamCloudVertex, List<RamCloudEdge>> vertexToEdgesMap = new HashMap<RamCloudVertex, List<RamCloudEdge>>();
    
    // Batch edges together by neighbor vertex
    for(RamCloudEdge edge: edges) {
      RamCloudVertex neighbor = (RamCloudVertex) edge.getNeighbor(this);
      List<RamCloudEdge> edgeList = vertexToEdgesMap.get(neighbor);
      
      if(edgeList == null)
        edgeList = new ArrayList<RamCloudEdge>();
      
      edgeList.add(edge);
      vertexToEdgesMap.put(neighbor, edgeList);
    }
    
    // Remove batches of edges at a time by neighbor vertex
    for(Entry<RamCloudVertex, List<RamCloudEdge>> entry: vertexToEdgesMap.entrySet()) {
      // Skip over loopback edges to ourself
      if(!entry.getKey().equals(this))
        entry.getKey().removeEdgesLocally(entry.getValue());
      
      // Remove this batch of edges from the edge property table
      for(RamCloudEdge edge: entry.getValue()) {
        edge.removeProperties();
      }
    }
    
    // Remove ourselves entirely from the vertex table
    graph.rcClient.remove(graph.vertTableId, rcKey);
    
    // Remove ourselves from our property table
    super.remove();
  }

  /*
   * Object overridden methods
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RamCloudVertex other = (RamCloudVertex) obj;
    if (id != other.id)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (id ^ (id >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "RamCloudVertex [id=" + id + "]";
  }

  /*
   * RamCloudVertex specific methods
   */ 
  private static byte[] idToRcKey(long id) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(id).array();
  }
  
  private static long rcKeyToId(byte[] rcKey) {
    return ByteBuffer.wrap(rcKey).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  public void addEdgeLocally(RamCloudEdge edge) {
    List<RamCloudEdge> edgesToAdd = new ArrayList<RamCloudEdge>();
    edgesToAdd.add(edge);
    addEdgesLocally(edgesToAdd);
  }
  
  public void addEdgesLocally(List<RamCloudEdge> edgesToAdd) {
    logger.log(Level.FINER, this + ": Adding edges: [edgesToAdd=" + edgesToAdd + "]");
    
    Set<RamCloudEdge> edges = getEdgeSet();
    
    try {
      if(edges.addAll(edgesToAdd))
        setEdgeSet(edges);
      else
        logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToAdd.toString() + ")");
    } catch(UnsupportedOperationException e) {
      logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToAdd.toString() + "): " + e.getLocalizedMessage());
    } catch(ClassCastException e) {
      logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToAdd.toString() + "): " + e.getLocalizedMessage());
    } catch(NullPointerException e) {
      logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToAdd.toString() + "): " + e.getLocalizedMessage());
    }
  }
  
  public void removeEdgeLocally(RamCloudEdge edge) {
    List<RamCloudEdge> edgesToRemove = new ArrayList<RamCloudEdge>();
    edgesToRemove.add(edge);
    removeEdgesLocally(edgesToRemove);
  }
   
   public void removeEdgesLocally(List<RamCloudEdge> edgesToRemove) {
     logger.log(Level.FINER, this + ": Removing edges: [edgesToRemove=" + edgesToRemove + "]");
     
     Set<RamCloudEdge> edges = getEdgeSet();
     
     try {
       if(edges.removeAll(edgesToRemove))
         setEdgeSet(edges);
       else
         logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToRemove.toString() + ")");
     } catch(UnsupportedOperationException e) {
       logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToRemove.toString() + "): " + e.getLocalizedMessage());
     } catch(ClassCastException e) {
       logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToRemove.toString() + "): " + e.getLocalizedMessage());
     } catch(NullPointerException e) {
       logger.log(Level.WARNING, toString() + ": Failed to remove a set of edges (" + edgesToRemove.toString() + "): " + e.getLocalizedMessage());
     }
   }

  public Set<RamCloudEdge> getEdgeSet() {
    return getEdgeSet(Direction.BOTH);
  }
  
  public Set<RamCloudEdge> getEdgeSet(Direction direction, String... labels) {
    JRamCloud.Object vertTableEntry;
    EdgeListProtoBuf edgeList;
    Set<RamCloudEdge> edgeSet = new HashSet<RamCloudEdge>();
    RamCloudVertex neighbor;
    
    try {
      vertTableEntry = graph.rcClient.read(graph.vertTableId, rcKey);
    } catch(Exception e) {
      logger.log(Level.WARNING, toString() + ": Error reading vertex table entry: " + e.toString());
      return null;
    }
    
    try {
      edgeList = EdgeListProtoBuf.parseFrom(vertTableEntry.value);
    } catch(InvalidProtocolBufferException e) {
      logger.log(Level.WARNING, toString() + ": Read malformed edge list: " + e.toString());
      return null;
    }
    
    for(EdgeProtoBuf edge: edgeList.getEdgeList()) {
      if( (direction.equals(Direction.BOTH) || (edge.getOutgoing() ^ direction.equals(Direction.IN))) && 
          (labels.length == 0 || Arrays.asList(labels).contains(edge.getLabel()))) {
        neighbor = new RamCloudVertex(edge.getNeighborId(), graph);
        if(edge.getOutgoing()) {
          edgeSet.add(new RamCloudEdge(this, neighbor, edge.getLabel(), graph));
        } else {
          edgeSet.add(new RamCloudEdge(neighbor, this, edge.getLabel(), graph));
        }
      }
    }
    
    return edgeSet;
  }
  
  public void setEdgeSet(Set<RamCloudEdge> edgeSet) {
    EdgeListProtoBuf.Builder edgeListBuilder = EdgeListProtoBuf.newBuilder();
    EdgeProtoBuf.Builder edgeBuilder = EdgeProtoBuf.newBuilder();
    
    for(Edge edge: edgeSet) {
      if(edge.getVertex(Direction.OUT).equals(this) || edge.getVertex(Direction.IN).equals(this)) {
        if(edge.getVertex(Direction.OUT).equals(edge.getVertex(Direction.IN))) {
          edgeBuilder.setNeighborId(id);
          edgeBuilder.setOutgoing(true);
          edgeBuilder.setLabel(edge.getLabel());
          edgeListBuilder.addEdge(edgeBuilder.build());
          
          edgeBuilder.setOutgoing(false);
          edgeListBuilder.addEdge(edgeBuilder.build());
        } else {
          if(edge.getVertex(Direction.OUT).equals(this)) {
            edgeBuilder.setNeighborId((Long) edge.getVertex(Direction.IN).getId());
            edgeBuilder.setOutgoing(true);
            edgeBuilder.setLabel(edge.getLabel());
            edgeListBuilder.addEdge(edgeBuilder.build());
          } else {
            edgeBuilder.setNeighborId((Long) edge.getVertex(Direction.OUT).getId());
            edgeBuilder.setOutgoing(false);
            edgeBuilder.setLabel(edge.getLabel());
            edgeListBuilder.addEdge(edgeBuilder.build());
          }
        }
      } else {
        logger.log(Level.WARNING, toString() + ": Tried to add an edge unowned by this vertex (" + edge.toString() + ")");
      }
    }
    
    graph.rcClient.write(graph.vertTableId, rcKey, edgeListBuilder.build().toByteArray());
  }
  
 public List<RamCloudEdge> getEdgeList() {
   return getEdgeList(Direction.BOTH);
 }
 
 public List<RamCloudEdge> getEdgeList(Direction direction, String... labels) {
   JRamCloud.Object vertTableEntry;
   EdgeListProtoBuf edgeListPB;
   List<RamCloudEdge> edgeList = new ArrayList<RamCloudEdge>();
   RamCloudVertex neighbor;
   
   try {
     vertTableEntry = graph.rcClient.read(graph.vertTableId, rcKey);
   } catch(Exception e) {
     logger.log(Level.WARNING, toString() + ": Error reading vertex table entry: " + e.getMessage());
     return null;
   }
   
   try {
     edgeListPB = EdgeListProtoBuf.parseFrom(vertTableEntry.value);
   } catch(InvalidProtocolBufferException e) {
     logger.log(Level.WARNING, toString() + ": Read malformed edge list: " + e.getMessage());
     return null;
   }
   
   for(EdgeProtoBuf edge: edgeListPB.getEdgeList()) {
     if( (direction.equals(Direction.BOTH) || (edge.getOutgoing() ^ direction.equals(Direction.IN))) && 
         (labels.length == 0 || Arrays.asList(labels).contains(edge.getLabel()))) {
       neighbor = new RamCloudVertex(edge.getNeighborId(), graph);
       if(edge.getOutgoing()) {
         edgeList.add(new RamCloudEdge(this, neighbor, edge.getLabel(), graph));
       } else {
         edgeList.add(new RamCloudEdge(neighbor, this, edge.getLabel(), graph));
       }
     }
   }
   
   return edgeList;
 }

  protected boolean exists() {
    boolean vertTableEntryExists = false;
    boolean vertPropTableEntryExists = false;
    
    try {
      graph.rcClient.read(graph.vertTableId, rcKey);
      vertTableEntryExists = true;
    } catch(Exception e) {
      // Vertex table entry does not exist
    }
    
    try {
      graph.rcClient.read(graph.vertPropTableId, rcKey);
      vertPropTableEntryExists = true;
    } catch(Exception e) {
      // Vertex property table entry does not exist
    }
    
    if(vertTableEntryExists && vertPropTableEntryExists) {
      return true;
    } else if(!vertTableEntryExists && !vertPropTableEntryExists) {
      return false;
    } else {
      logger.log(Level.WARNING, toString() + ": Detected RamCloudGraph inconsistency: vertTableEntryExists=" + vertTableEntryExists + ", vertPropTableEntryExists=" + vertPropTableEntryExists + ".");
      return true;
    }
  }
  
  protected void create() throws IllegalArgumentException {
    // TODO: Existence check costs extra (presently 2 reads), could use option to turn on/off
    if(!exists()) {
      graph.rcClient.write(graph.vertTableId, rcKey, ByteBuffer.allocate(0).array());
      graph.rcClient.write(graph.vertPropTableId, rcKey, ByteBuffer.allocate(0).array());
    } else {
      throw ExceptionFactory.vertexWithIdAlreadyExists(id);
    }
  }
  
  public void debugPrintEdgeList() {
    List<RamCloudEdge> edgeList = getEdgeList();
    
    System.out.println(toString() + ": Debug Printing Edge List...");
    for(RamCloudEdge edge: edgeList) {
      System.out.println(edge.toString());
    }
  }
}
