package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

public class RamCloudElement implements Element {

  private static final Logger logger = Logger.getLogger(RamCloudGraph.class.getName());
  
  private byte[] rcPropTableKey;
  private long rcPropTableId;
  private JRamCloud rcClient;
  
  public RamCloudElement(byte[] rcPropTableKey, long rcPropTableId, JRamCloud rcClient) {
    this.rcPropTableKey = rcPropTableKey;
    this.rcPropTableId = rcPropTableId;
    this.rcClient = rcClient;
  }
  
  public Map<String, Object> getPropertyMap() {
    JRamCloud.Object propTableEntry;
    
    try {
      propTableEntry = rcClient.read(rcPropTableId, rcPropTableKey);
    } catch(Exception e) {
      logger.log(Level.WARNING, "Element does not have a property table entry!");
      return null;
    }
    
    return getPropertyMap(propTableEntry.value);
  }

  public static Map<String, Object> getPropertyMap(byte[] byteArray) {
    if(byteArray == null) {
      logger.log(Level.WARNING, "Got a null byteArray argument");
      return null;
    } else if(byteArray.length != 0) {
      try {
        ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Map<String, Object> map = (Map<String, Object>)ois.readObject();
        return map;
      } catch(IOException e) {
        logger.log(Level.WARNING, "Got an exception while deserializing element's property map: " + e.toString());
        return null;
      } catch(ClassNotFoundException e) {
        logger.log(Level.WARNING, "Got an exception while deserializing element's property map: " + e.toString());
        return null;
      }
    } else {
      return new HashMap<String, Object>();
    }
  }
  
  public void setPropertyMap(Map<String, Object> map) {
    byte[] rcValue;
    
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oot = new ObjectOutputStream(baos);
      oot.writeObject(map);
      rcValue = baos.toByteArray();
    } catch(IOException e) {
      logger.log(Level.WARNING, "Got an exception while serializing element's property map: " + e.toString());
      return;
    }
    
    rcClient.write(rcPropTableId, rcPropTableKey, rcValue);
  }
  
  @Override
  public <T> T getProperty(String key) {
    Map<String, Object> map = getPropertyMap();
    return (T)map.get(key);
  }

  @Override
  public Set<String> getPropertyKeys() {
    Map<String, Object> map = getPropertyMap();
    return map.keySet();
  }

  @Override
  public void setProperty(String key, Object value) {
    if(value == null) {
      throw ExceptionFactory.propertyValueCanNotBeNull();
    }
    
    if(key == null) {
      throw ExceptionFactory.propertyKeyCanNotBeNull();
    }
    
    if(key.equals("")) {
      throw ExceptionFactory.propertyKeyCanNotBeEmpty();
    }
    
    if(key.equals("id")) {
      throw ExceptionFactory.propertyKeyIdIsReserved();
    }
    
    if(this instanceof RamCloudEdge && key.equals("label")) {
      throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
    }
    
    Map<String, Object> map = getPropertyMap();
    map.put(key, value);
    setPropertyMap(map);
  }

  @Override
  public <T> T removeProperty(String key) {
    Map<String, Object> map = getPropertyMap();
    T retVal = (T)map.remove(key);
    setPropertyMap(map);
    return retVal;
  }

  @Override
  public void remove() {
    rcClient.remove(rcPropTableId, rcPropTableKey);
  }

  @Override
  public Object getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toString() {
    return "RamCloudElement [rcPropTableKey=" + Arrays.toString(rcPropTableKey)
        + ", rcPropTableId=" + rcPropTableId + "]";
  }

}
