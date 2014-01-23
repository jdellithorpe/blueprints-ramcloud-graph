/* Copyright (c) 2013 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.jersey.core.util.Base64;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

public class RamCloudEdge extends RamCloudElement implements Edge {

  private static final Logger logger = Logger.getLogger(RamCloudGraph.class.getName());
  
  private RamCloudVertex outVertex;
  private RamCloudVertex inVertex;
  private String label;
  private byte[] rcKey;
  private RamCloudGraph graph;
  
  public RamCloudEdge(RamCloudVertex outVertex, RamCloudVertex inVertex, String label, RamCloudGraph graph) {
    super(edgeToRcKey(outVertex, inVertex, label), graph.edgePropTableId, graph.rcClient);
    
    this.outVertex = outVertex;
    this.inVertex = inVertex;
    this.label = label;
    this.rcKey = edgeToRcKey(outVertex, inVertex, label);
    this.graph = graph;
  }
  
  public RamCloudEdge(byte[] rcKey, RamCloudGraph graph) {
    super(rcKey, graph.edgePropTableId, graph.rcClient);
    
    ByteBuffer edgeId = ByteBuffer.wrap(rcKey).order(ByteOrder.LITTLE_ENDIAN);
    outVertex = new RamCloudVertex(edgeId.getLong(), graph);
    inVertex = new RamCloudVertex(edgeId.getLong(), graph);
    label = new String(rcKey, 16, rcKey.length - 16);
    
    this.rcKey = rcKey;
    this.graph = graph;
  }
  
  private static byte[] edgeToRcKey(RamCloudVertex outVertex, RamCloudVertex inVertex, String label) {
    return ByteBuffer.allocate(16 + label.length()).order(ByteOrder.LITTLE_ENDIAN)
                      .putLong((Long) outVertex.getId())
                      .putLong((Long) inVertex.getId())
                      .put(label.getBytes())
                      .array();
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

  public boolean isLoop() {
    return outVertex.equals(inVertex);
  }

  public Vertex getNeighbor(Vertex vertex) {
    if(outVertex.equals(vertex))
      return inVertex;
    else if(inVertex.equals(vertex))
      return outVertex;
    else
      return null;
  }
  
  @Override
  public void remove() {
    if(isLoop()) {
      outVertex.removeEdgeLocally(this);
    } else {
      outVertex.removeEdgeLocally(this);
      inVertex.removeEdgeLocally(this);
    }
    
    super.remove();
  }
  
  public void removeProperties() {
    super.remove();
  }

  @Override
  public Object getId() {
    return (Object)new String(Base64.encode(rcKey));
  }
  
  public boolean exists() {
    boolean edgePropTableEntryExists;
    boolean outVertexEntryExists;
    boolean inVertexEntryExists;
    
    try {
      graph.rcClient.read(graph.edgePropTableId, rcKey);
      edgePropTableEntryExists = true;
    } catch(Exception e) {
      // Edge property table entry does not exist
      edgePropTableEntryExists = false;
    }
    
    outVertexEntryExists = outVertex.getEdgeSet().contains(this);
    
    if(!outVertex.equals(inVertex))
      inVertexEntryExists = inVertex.getEdgeSet().contains(this);
    else
      inVertexEntryExists = outVertexEntryExists;
    
    if(edgePropTableEntryExists && outVertexEntryExists && inVertexEntryExists) {
      return true;
    } else if(!edgePropTableEntryExists && !outVertexEntryExists && !inVertexEntryExists) {
      return false;
    } else {
      logger.log(Level.WARNING, toString() + ": Detected RamCloudGraph inconsistency: inVertexEntryExists=" + inVertexEntryExists + ", outVertexEntryExists=" + outVertexEntryExists + ", inVertexEntryExists=" + inVertexEntryExists + ".");
      return true;
    }
  }
  
  public void create() throws IllegalArgumentException {
    // TODO: Existence check costs extra (presently 3 reads), could use option to turn on/off
    if(!exists()) {
      outVertex.addEdgeLocally(this);
      if(!isLoop())
        inVertex.addEdgeLocally(this);
      
      graph.rcClient.write(graph.edgePropTableId, rcKey, ByteBuffer.allocate(0).array());
    } else
      throw ExceptionFactory.edgeWithIdAlreadyExist(rcKey);
  }
  
  public static boolean isValidEdgeId(byte[] id) {
    if(id == null)
      return false;
    if(id.length == 0)
      return false;
    
    ByteBuffer edgeId = ByteBuffer.wrap(id);
    try {
      edgeId.getLong();
    } catch(BufferUnderflowException e) {
      return false;
    }
    
    try {
      edgeId.getLong();
    } catch(BufferUnderflowException e) {
      return false;
    }
    
    if(edgeId.remaining() == 0)
      return false;
    
    return true;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(rcKey);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RamCloudEdge other = (RamCloudEdge) obj;
    if (!Arrays.equals(rcKey, other.rcKey))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "RamCloudEdge [outVertex=" + outVertex + ", inVertex=" + inVertex
        + ", label=" + label + "]";
  }
}
